/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use async_recursion::async_recursion;
use std::collections::HashMap;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::task_graph::cache::VegaFusionCache;
use crate::task_graph::task::TaskCall;
use crate::task_graph::timezone::RuntimeTzConfig;
use futures_util::{future, FutureExt};
use prost::Message as ProstMessage;
use serde_json::Value;
use std::convert::{TryFrom, TryInto};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::planning::plan::{PlannerConfig, SpecPlan};
use vegafusion_core::planning::watch::{ExportUpdate, ExportUpdateNamespace};
use vegafusion_core::proto::gen::errors::error::Errorkind;
use vegafusion_core::proto::gen::errors::{Error, TaskGraphValueError};
use vegafusion_core::proto::gen::pretransform::pre_transform_spec_warning::WarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValuesWarningType;
use vegafusion_core::proto::gen::pretransform::{
    PlannerWarning, PreTransformSpecWarning, PreTransformValuesRequest, PreTransformValuesResponse,
    PreTransformValuesWarning,
};
use vegafusion_core::proto::gen::pretransform::{
    PreTransformBrokenInteractivityWarning, PreTransformRowLimitWarning, PreTransformSpecRequest,
    PreTransformSpecResponse, PreTransformUnsupportedWarning,
};
use vegafusion_core::proto::gen::services::{
    pre_transform_spec_result, pre_transform_values_result, query_request, query_result,
    PreTransformSpecResult, PreTransformValuesResult, QueryRequest, QueryResult,
};
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueResponse,
    TaskValue as ProtoTaskValue, TzConfig, Variable, VariableNamespace,
};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;

type CacheValue = (TaskValue, Vec<TaskValue>);

#[derive(Clone)]
pub struct TaskGraphRuntime {
    pub cache: VegaFusionCache,
}

impl TaskGraphRuntime {
    pub fn new(capacity: Option<usize>, memory_limit: Option<usize>) -> Self {
        Self {
            cache: VegaFusionCache::new(capacity, memory_limit),
        }
    }

    pub async fn get_node_value(
        &self,
        task_graph: Arc<TaskGraph>,
        node_value_index: &NodeValueIndex,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<TaskValue> {
        // We shouldn't panic inside get_or_compute_node_value, but since this may be used
        // in a server context, wrap in catch_unwind just in case.
        let node_value = AssertUnwindSafe(get_or_compute_node_value(
            task_graph,
            node_value_index.node_index as usize,
            self.cache.clone(),
            inline_datasets,
        ))
        .catch_unwind()
        .await;

        let mut node_value = node_value
            .ok()
            .with_context(|| "Unknown panic".to_string())??;

        Ok(match node_value_index.output_index {
            None => node_value.0,
            Some(output_index) => node_value.1.remove(output_index as usize),
        })
    }

    pub async fn query_request(&self, request: QueryRequest) -> Result<QueryResult> {
        match request.request {
            Some(query_request::Request::TaskGraphValues(task_graph_values)) => {
                let task_graph = Arc::new(task_graph_values.task_graph.unwrap());

                // Clone task_graph and task_graph_runtime for use in closure
                let task_graph_runtime = self.clone();
                let task_graph = task_graph.clone();

                let response_value_futures: Vec<_> = task_graph_values
                    .indices
                    .iter()
                    .map(|node_value_index| {
                        let node = task_graph
                            .nodes
                            .get(node_value_index.node_index as usize)
                            .with_context(|| {
                                format!(
                                    "Node index {} out of bounds for graph with size {}",
                                    node_value_index.node_index,
                                    task_graph.nodes.len()
                                )
                            })?;
                        let task = node.task();
                        let var = match node_value_index.output_index {
                            None => task.variable().clone(),
                            Some(output_index) => task.output_vars()[output_index as usize].clone(),
                        };

                        let scope = node.task().scope.clone();

                        // Clone task_graph and task_graph_runtime for use in closure
                        let task_graph_runtime = task_graph_runtime.clone();
                        let task_graph = task_graph.clone();

                        Ok(async move {
                            let value = task_graph_runtime
                                .clone()
                                .get_node_value(task_graph, node_value_index, Default::default())
                                .await?;

                            Ok::<_, VegaFusionError>(ResponseTaskValue {
                                variable: Some(var),
                                scope,
                                value: Some(ProtoTaskValue::try_from(&value).unwrap()),
                            })
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                match future::try_join_all(response_value_futures).await {
                    Ok(response_values) => {
                        let response_msg = QueryResult {
                            response: Some(query_result::Response::TaskGraphValues(
                                TaskGraphValueResponse { response_values },
                            )),
                        };
                        Ok(response_msg)
                    }
                    Err(e) => {
                        let response_msg = QueryResult {
                            response: Some(query_result::Response::Error(Error {
                                errorkind: Some(Errorkind::Error(TaskGraphValueError {
                                    msg: e.to_string(),
                                })),
                            })),
                        };
                        Ok(response_msg)
                    }
                }
            }
            _ => Err(VegaFusionError::internal(
                "Invalid VegaFusionRuntimeRequest request",
            )),
        }
    }

    /// request_bytes should be encoding of a VegaFusionRuntimeRequest
    /// returned value is encoding of a VegaFusionRuntimeResponse
    pub async fn query_request_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        // Decode request
        let request = QueryRequest::decode(request_bytes).unwrap();
        let response_msg = self.query_request(request).await?;

        let mut buf: Vec<u8> = Vec::new();
        buf.reserve(response_msg.encoded_len());
        response_msg
            .encode(&mut buf)
            .external("Failed to encode response")?;
        Ok(buf)
    }

    pub async fn pre_transform_spec_request(
        &self,
        request: PreTransformSpecRequest,
    ) -> Result<PreTransformSpecResult> {
        // Get row limit
        let row_limit = request.opts.as_ref().and_then(|opts| opts.row_limit);

        // Extract and deserialize inline datasets
        let inline_pretransform_datasets = request
            .opts
            .map(|opts| opts.inline_datasets)
            .unwrap_or_default();

        let inline_datasets = inline_pretransform_datasets
            .iter()
            .map(|inline_dataset| {
                let dataset = VegaFusionDataset::from_table_ipc_bytes(&inline_dataset.table)?;
                Ok((inline_dataset.name.clone(), dataset))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        // Parse spec
        let spec_string = request.spec;
        let local_tz = request.local_tz;
        let output_tz = request.output_tz;

        self.pre_transform_spec(
            &spec_string,
            &local_tz,
            &output_tz,
            row_limit,
            inline_datasets,
        )
        .await
    }

    pub async fn pre_transform_spec(
        &self,
        spec: &str,
        local_tz: &str,
        default_input_tz: &Option<String>,
        row_limit: Option<u32>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<PreTransformSpecResult> {
        let spec: ChartSpec =
            serde_json::from_str(spec).with_context(|| "Failed to parse spec".to_string())?;

        // Create spec plan
        let plan = SpecPlan::try_new(
            &spec,
            &PlannerConfig {
                stringify_local_datetimes: true,
                extract_inline_data: true,
                ..Default::default()
            },
        )?;

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &inline_datasets)
            .unwrap();
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        // Gather values of server-to-client values
        let mut init = Vec::new();
        for var in &plan.comm_plan.server_to_client {
            let node_index = task_graph_mapping
                .get(var)
                .unwrap_or_else(|| panic!("Failed to lookup variable '{:?}'", var));
            let value = self
                .get_node_value(
                    Arc::new(task_graph.clone()),
                    node_index,
                    inline_datasets.clone(),
                )
                .await
                .expect("Failed to get node value");

            init.push(ExportUpdate {
                namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
                name: var.0.name.clone(),
                scope: var.1.clone(),
                value: value.to_json().unwrap(),
            });
        }

        // Update client spec with server values
        let mut spec = plan.client_spec.clone();
        let mut limited_datasets: Vec<Variable> = Vec::new();
        for export_update in init {
            let scope = export_update.scope.clone();
            let name = export_update.name.as_str();
            match export_update.namespace {
                ExportUpdateNamespace::Signal => {
                    let signal = spec.get_nested_signal_mut(&scope, name)?;
                    signal.value = Some(export_update.value);
                }
                ExportUpdateNamespace::Data => {
                    let data = spec.get_nested_data_mut(&scope, name)?;
                    // Handle row_limit
                    let value = if let Value::Array(values) = &export_update.value {
                        if let Some(row_limit) = row_limit {
                            let row_limit = row_limit as usize;
                            if values.len() > row_limit {
                                limited_datasets.push(export_update.to_scoped_var().0);
                                Value::Array(Vec::from(&values[..row_limit]))
                            } else {
                                Value::Array(values.clone())
                            }
                        } else {
                            Value::Array(values.clone())
                        }
                    } else {
                        return Err(VegaFusionError::internal(
                            "Expected Data value to be an Array",
                        ));
                    };

                    // Set inline value
                    // Other properties are cleared by planning process so we don't alter them here
                    data.values = Some(value);
                }
            }
        }

        // Build warnings
        let mut warnings: Vec<PreTransformSpecWarning> = Vec::new();

        // Add unsupported warning (
        if plan.comm_plan.server_to_client.is_empty() {
            warnings.push(PreTransformSpecWarning {
                warning_type: Some(WarningType::Unsupported(PreTransformUnsupportedWarning {})),
            });
        }

        // Add Row Limit warning
        if !limited_datasets.is_empty() {
            warnings.push(PreTransformSpecWarning {
                warning_type: Some(WarningType::RowLimit(PreTransformRowLimitWarning {
                    datasets: limited_datasets,
                })),
            });
        }

        // Add Broken Interactivity warning
        if !plan.comm_plan.client_to_server.is_empty() {
            let vars: Vec<_> = plan
                .comm_plan
                .client_to_server
                .iter()
                .map(|var| var.0.clone())
                .collect();
            warnings.push(PreTransformSpecWarning {
                warning_type: Some(WarningType::BrokenInteractivity(
                    PreTransformBrokenInteractivityWarning { vars },
                )),
            });
        }

        // Add planner warnings
        for planner_warning in &plan.warnings {
            warnings.push(PreTransformSpecWarning {
                warning_type: Some(WarningType::Planner(PlannerWarning {
                    message: planner_warning.message(),
                })),
            });
        }

        // Build result
        let response = PreTransformSpecResult {
            result: Some(pre_transform_spec_result::Result::Response(
                PreTransformSpecResponse {
                    spec: serde_json::to_string(&spec)
                        .expect("Failed to convert chart spec to string"),
                    warnings,
                },
            )),
        };

        Ok(response)
    }

    pub async fn pre_transform_values_request(
        &self,
        request: PreTransformValuesRequest,
    ) -> Result<PreTransformValuesResult> {
        // Extract and deserialize inline datasets
        let inline_pretransform_datasets = request
            .opts
            .clone()
            .map(|opts| opts.inline_datasets)
            .unwrap_or_default();

        let inline_datasets = inline_pretransform_datasets
            .iter()
            .map(|inline_dataset| {
                let dataset = VegaFusionDataset::from_table_ipc_bytes(&inline_dataset.table)?;
                Ok((inline_dataset.name.clone(), dataset))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        // Extract requested variables
        let variables: Vec<ScopedVariable> = request
            .opts
            .map(|opts| opts.variables)
            .unwrap_or_default()
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();

        // Parse spec
        let spec_string = request.spec;
        let local_tz = request.local_tz;
        let default_input_tz = request.default_input_tz;

        let (values, warnings) = self
            .pre_transform_values(
                &spec_string,
                variables.as_slice(),
                &local_tz,
                &default_input_tz,
                inline_datasets,
            )
            .await?;

        let response_values: Vec<_> = values
            .iter()
            .zip(&variables)
            .map(|(value, var)| {
                let proto_value = ProtoTaskValue::try_from(value)?;
                Ok(ResponseTaskValue {
                    variable: Some(var.0.clone()),
                    scope: var.1.clone(),
                    value: Some(proto_value),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build result
        let result = PreTransformValuesResult {
            result: Some(pre_transform_values_result::Result::Response(
                PreTransformValuesResponse {
                    values: response_values,
                    warnings,
                },
            )),
        };

        Ok(result)
    }

    pub async fn pre_transform_values(
        &self,
        spec: &str,
        variables: &[ScopedVariable],
        local_tz: &str,
        default_input_tz: &Option<String>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<(Vec<TaskValue>, Vec<PreTransformValuesWarning>)> {
        let spec: ChartSpec =
            serde_json::from_str(spec).with_context(|| "Failed to parse spec".to_string())?;

        // Check that requested variables exist
        for var in variables {
            let scope = var.1.as_slice();
            match &var.0.ns() {
                VariableNamespace::Signal => {
                    if spec.get_nested_signal(scope, &var.0.name).is_err() {
                        return Err(VegaFusionError::pre_transform(format!(
                            "No signal named {} with scope {:?}",
                            var.0.name, scope
                        )));
                    }
                }
                VariableNamespace::Data => {
                    if spec.get_nested_data(scope, &var.0.name).is_err() {
                        return Err(VegaFusionError::pre_transform(format!(
                            "No dataset named {} with scope {:?}",
                            var.0.name, scope
                        )));
                    }
                }
                VariableNamespace::Scale => {
                    return Err(VegaFusionError::pre_transform(format!(
                        "pre_transform_values does not support scale variable {:?}",
                        var.0
                    )))
                }
            }
        }

        // Create spec plan
        let plan = SpecPlan::try_new(
            &spec,
            &PlannerConfig {
                stringify_local_datetimes: true,
                extract_inline_data: true,
                split_domain_data: false,
                projection_pushdown: false,
                ..Default::default()
            },
        )?;

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &inline_datasets)
            .unwrap();
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        let mut warnings: Vec<PreTransformValuesWarning> = Vec::new();

        // Add planner warnings
        for planner_warning in &plan.warnings {
            warnings.push(PreTransformValuesWarning {
                warning_type: Some(ValuesWarningType::Planner(PlannerWarning {
                    message: planner_warning.message(),
                })),
            });
        }

        // Gather the values of requested variables
        let mut values: Vec<TaskValue> = Vec::new();
        for var in variables {
            let node_index = if let Some(node_index) = task_graph_mapping.get(var) {
                node_index
            } else {
                return Err(VegaFusionError::pre_transform(format!(
                    "Requested variable {:?}\n requires transforms or signal \
                        expressions that are not yet supported",
                    var
                )));
            };

            let value = self
                .get_node_value(
                    Arc::new(task_graph.clone()),
                    node_index,
                    inline_datasets.clone(),
                )
                .await?;
            values.push(value);
        }

        Ok((values, warnings))
    }

    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }
}

#[async_recursion]
async fn get_or_compute_node_value(
    task_graph: Arc<TaskGraph>,
    node_index: usize,
    cache: VegaFusionCache,
    inline_datasets: HashMap<String, VegaFusionDataset>,
) -> Result<CacheValue> {
    // Get the cache key for requested node
    let node = task_graph.node(node_index).unwrap();
    let task = node.task();

    if let TaskKind::Value(value) = task.task_kind() {
        // Root nodes are stored in the graph, so we don't add them to the cache
        Ok((value.try_into().unwrap(), Vec::new()))
    } else {
        // Collect input node indices
        let input_node_indexes = task_graph.parent_indices(node_index).unwrap();
        let input_edges = node.incoming.clone();

        // Clone task so we can move it to async block
        let task = task.clone();
        let tz_config = task.tz_config.clone().and_then(|tz_config| {
            RuntimeTzConfig::try_new(&tz_config.local_tz, &tz_config.default_input_tz).ok()
        });

        let cache_key = node.state_fingerprint;
        let cloned_cache = cache.clone();

        let fut = async move {
            // Create future to compute node value (will only be executed if not present in cache)
            let mut inputs_futures = Vec::new();
            for input_node_index in input_node_indexes {
                inputs_futures.push(tokio::spawn(get_or_compute_node_value(
                    task_graph.clone(),
                    input_node_index,
                    cloned_cache.clone(),
                    inline_datasets.clone(),
                )));
            }

            let input_values = futures::future::join_all(inputs_futures).await;

            // Extract the appropriate value from
            let input_values = input_values
                .into_iter()
                .zip(input_edges)
                .map(|(value, edge)| {
                    // Convert outer JoinHandle error to internal VegaFusionError so we can propagate it.
                    let mut value = match value {
                        Ok(value) => value?,
                        Err(join_err) => {
                            return Err(VegaFusionError::internal(&join_err.to_string()))
                        }
                    };

                    let value = match edge.output {
                        None => value.0,
                        Some(output_index) => value.1.remove(output_index as usize),
                    };
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;

            task.eval(&input_values, &tz_config, inline_datasets).await
        };

        // get or construct from cache

        cache.get_or_try_insert_with(cache_key, fut).await
    }
}
