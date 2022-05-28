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
use futures_util::{future, FutureExt};
use prost::Message as ProstMessage;
use serde_json::Value;
use std::convert::{TryFrom, TryInto};
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::planning::plan::{PlannerConfig, SpecPlan};
use vegafusion_core::planning::stringify_local_datetimes::OutputLocalDatetimesConfig;
use vegafusion_core::planning::watch::{ExportUpdate, ExportUpdateNamespace};
use vegafusion_core::proto::gen::errors::error::Errorkind;
use vegafusion_core::proto::gen::errors::{Error, TaskGraphValueError};
use vegafusion_core::proto::gen::pretransform::pre_transform_warning::WarningType;
use vegafusion_core::proto::gen::pretransform::PreTransformWarning;
use vegafusion_core::proto::gen::pretransform::{
    PreTransformBrokenInteractivityWarning, PreTransformRequest, PreTransformResponse,
    PreTransformRowLimitWarning, PreTransformUnsupportedWarning,
};
use vegafusion_core::proto::gen::services::{
    pre_transform_result, query_request, query_result, PreTransformResult, QueryRequest,
    QueryResult,
};
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueResponse,
    TaskValue as ProtoTaskValue, Variable,
};
use vegafusion_core::spec::chart::ChartSpec;

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
    ) -> Result<TaskValue> {
        // We shouldn't panic inside get_or_compute_node_value, but since this may be used
        // in a server context, wrap in catch_unwind just in case.
        let node_value = AssertUnwindSafe(get_or_compute_node_value(
            task_graph,
            node_value_index.node_index as usize,
            self.cache.clone(),
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
                                .get_node_value(task_graph, node_value_index)
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
        request: PreTransformRequest,
    ) -> Result<PreTransformResult> {
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
                let table = VegaFusionTable::from_ipc_bytes(&inline_dataset.table)?;
                Ok((inline_dataset.name.clone(), table))
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
        output_tz: &Option<String>,
        row_limit: Option<u32>,
        inline_datasets: HashMap<String, VegaFusionTable>,
    ) -> Result<PreTransformResult> {
        let spec: ChartSpec =
            serde_json::from_str(spec).with_context(|| "Failed to parse spec".to_string())?;

        // Create spec plan
        let local_datetimes_config = match output_tz {
            None => OutputLocalDatetimesConfig::LocalNaiveString,
            Some(output_tz) => OutputLocalDatetimesConfig::TimezoneNaiveString(output_tz.clone()),
        };

        let plan = SpecPlan::try_new(
            &spec,
            &PlannerConfig {
                local_datetimes_config,
                extract_inline_data: true,
                ..Default::default()
            },
        )?;

        // Create task graph for server spec
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(local_tz, Some(inline_datasets))
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
                .get_node_value(Arc::new(task_graph.clone()), node_index)
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
        let mut warnings: Vec<PreTransformWarning> = Vec::new();

        // Add unsupported warning (
        if plan.comm_plan.server_to_client.is_empty() {
            warnings.push(PreTransformWarning {
                warning_type: Some(WarningType::Unsupported(PreTransformUnsupportedWarning {})),
            });
        }

        // Add Row Limit warning
        if !limited_datasets.is_empty() {
            warnings.push(PreTransformWarning {
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
            warnings.push(PreTransformWarning {
                warning_type: Some(WarningType::BrokenInteractivity(
                    PreTransformBrokenInteractivityWarning { vars },
                )),
            });
        }

        // Build result
        let response = PreTransformResult {
            result: Some(pre_transform_result::Result::Response(
                PreTransformResponse {
                    spec: serde_json::to_string(&spec)
                        .expect("Failed to convert chart spec to string"),
                    warnings,
                },
            )),
        };

        Ok(response)
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
        let local_tz = task
            .local_tz
            .as_ref()
            .and_then(|tz| chrono_tz::Tz::from_str(tz).ok());
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

            task.eval(&input_values, &local_tz).await
        };

        // get or construct from cache
        let result = cache.get_or_try_insert_with(cache_key, fut).await;

        result
    }
}
