use crate::task_graph::cache::VegaFusionCache;
use crate::task_graph::task::TaskCall;
use crate::task_graph::timezone::RuntimeTzConfig;
use async_recursion::async_recursion;
use futures_util::{future, FutureExt};
use prost::Message as ProstMessage;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::planning::apply_pre_transform::apply_pre_transform_datasets;
use vegafusion_core::planning::destringify_selection_datetimes::destringify_selection_datetimes;
use vegafusion_core::planning::plan::{PlannerConfig, SpecPlan};
use vegafusion_core::planning::watch::{ExportUpdateArrow, ExportUpdateNamespace};
use vegafusion_core::proto::gen::errors::error::Errorkind;
use vegafusion_core::proto::gen::errors::{Error, TaskGraphValueError};
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValuesWarningType;
use vegafusion_core::proto::gen::pretransform::{
    pre_transform_extract_warning, PlannerWarning,
    PreTransformExtractDataset as ProtoPreTransformExtractDataset, PreTransformExtractRequest,
    PreTransformExtractResponse, PreTransformExtractWarning, PreTransformSpecWarning,
    PreTransformValuesRequest, PreTransformValuesResponse, PreTransformValuesWarning,
};
use vegafusion_core::proto::gen::pretransform::{
    PreTransformRowLimitWarning, PreTransformSpecRequest, PreTransformSpecResponse,
};
use vegafusion_core::proto::gen::services::{
    pre_transform_extract_result, pre_transform_spec_result, pre_transform_values_result,
    query_request, query_result, PreTransformExtractResult, PreTransformSpecResult,
    PreTransformValuesResult, QueryRequest, QueryResult,
};
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, InlineDataset, NodeValueIndex, ResponseTaskValue, TaskGraph,
    TaskGraphValueResponse, TaskValue as ProtoTaskValue, TzConfig, VariableNamespace,
};
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::spec::values::MissingNullOrValue;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::connection::Connection;

type CacheValue = (TaskValue, Vec<TaskValue>);
type PreTransformExtractDataset = (String, Vec<u32>, VegaFusionTable);

#[derive(Clone)]
pub struct VegaFusionRuntime {
    pub cache: VegaFusionCache,
    pub conn: Arc<dyn Connection>,
}

impl VegaFusionRuntime {
    pub fn new(
        conn: Arc<dyn Connection>,
        capacity: Option<usize>,
        memory_limit: Option<usize>,
    ) -> Self {
        Self {
            cache: VegaFusionCache::new(capacity, memory_limit),
            conn,
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
            self.conn.clone(),
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

    pub async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<ResponseTaskValue>> {
        // Clone task_graph and task_graph_runtime for use in closure
        let task_graph_runtime = self.clone();
        let response_value_futures: Vec<_> = indices
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
                        .get_node_value(task_graph, node_value_index, inline_datasets.clone())
                        .await?;

                    Ok::<_, VegaFusionError>(ResponseTaskValue {
                        variable: Some(var),
                        scope,
                        value: Some(ProtoTaskValue::try_from(&value).unwrap()),
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;

        future::try_join_all(response_value_futures).await
    }

    pub async fn query_request_message(&self, request: QueryRequest) -> Result<QueryResult> {
        match request.request {
            Some(query_request::Request::TaskGraphValues(task_graph_values)) => {
                let task_graph = Arc::new(task_graph_values.task_graph.unwrap());
                let indices = &task_graph_values.indices;
                let inline_datasets =
                    Self::decode_inline_datasets(task_graph_values.inline_datasets)?;

                match self
                    .query_request(task_graph, indices.as_slice(), &inline_datasets)
                    .await
                {
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
        let response_msg = self.query_request_message(request).await?;

        let mut buf: Vec<u8> = Vec::with_capacity(response_msg.encoded_len());
        response_msg
            .encode(&mut buf)
            .external("Failed to encode response")?;
        Ok(buf)
    }

    pub async fn pre_transform_spec_request(
        &self,
        request: PreTransformSpecRequest,
    ) -> Result<PreTransformSpecResult> {
        // Extract options
        let (row_limit, preserve_interactivity, inline_pretransform_datasets, keep_variables) =
            if let Some(opts) = request.opts {
                // Convert keep_variables to ScopedVariable
                let keep_variables: Vec<ScopedVariable> = opts
                    .keep_variables
                    .into_iter()
                    .map(|var| (var.variable.unwrap(), var.scope))
                    .collect();
                (
                    opts.row_limit,
                    opts.preserve_interactivity,
                    opts.inline_datasets,
                    keep_variables,
                )
            } else {
                (None, true, Default::default(), Default::default())
            };

        let inline_datasets = Self::decode_inline_datasets(inline_pretransform_datasets)?;

        // Parse spec
        let spec: ChartSpec = serde_json::from_str(&request.spec)?;
        let local_tz = request.local_tz;
        let output_tz = request.output_tz;

        let (transformed_spec, warnings) = self
            .pre_transform_spec(
                &spec,
                &local_tz,
                &output_tz,
                row_limit,
                preserve_interactivity,
                inline_datasets,
                keep_variables,
            )
            .await?;

        // Build result
        let response = PreTransformSpecResult {
            result: Some(pre_transform_spec_result::Result::Response(
                PreTransformSpecResponse {
                    spec: serde_json::to_string(&transformed_spec)
                        .with_context(|| "Failed to convert chart spec to string")?,
                    warnings,
                },
            )),
        };

        Ok(response)
    }

    fn decode_inline_datasets(
        inline_pretransform_datasets: Vec<InlineDataset>,
    ) -> Result<HashMap<String, VegaFusionDataset>> {
        let inline_datasets = inline_pretransform_datasets
            .iter()
            .map(|inline_dataset| {
                let dataset = VegaFusionDataset::from_table_ipc_bytes(&inline_dataset.table)?;
                Ok((inline_dataset.name.clone(), dataset))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(inline_datasets)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn pre_transform_spec(
        &self,
        spec: &ChartSpec,
        local_tz: &str,
        default_input_tz: &Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: bool,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        keep_variables: Vec<ScopedVariable>,
    ) -> Result<(ChartSpec, Vec<PreTransformSpecWarning>)> {
        let input_spec = spec;

        let (plan, init) = self
            .perform_pre_transform_spec(
                spec,
                local_tz,
                default_input_tz,
                preserve_interactivity,
                inline_datasets,
                keep_variables,
            )
            .await?;

        apply_pre_transform_datasets(input_spec, &plan, init, row_limit)
    }

    pub async fn pre_transform_values_request(
        &self,
        request: PreTransformValuesRequest,
    ) -> Result<PreTransformValuesResult> {
        // Extract row limit
        let row_limit = request.opts.as_ref().and_then(|opts| opts.row_limit);

        // Extract and deserialize inline datasets
        let inline_pretransform_datasets = request
            .opts
            .clone()
            .map(|opts| opts.inline_datasets)
            .unwrap_or_default();

        let inline_datasets = Self::decode_inline_datasets(inline_pretransform_datasets)?;

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
        let spec: ChartSpec = serde_json::from_str(&spec_string)?;
        let local_tz = request.local_tz;
        let default_input_tz = request.default_input_tz;

        let (values, warnings) = self
            .pre_transform_values(
                &spec,
                variables.as_slice(),
                &local_tz,
                &default_input_tz,
                row_limit,
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
        spec: &ChartSpec,
        variables: &[ScopedVariable],
        local_tz: &str,
        default_input_tz: &Option<String>,
        row_limit: Option<u32>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<(Vec<TaskValue>, Vec<PreTransformValuesWarning>)> {
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
            spec,
            &PlannerConfig {
                stringify_local_datetimes: false,
                extract_inline_data: true,
                split_domain_data: false,
                projection_pushdown: false,
                allow_client_to_server_comms: true,
                keep_variables: Vec::from(variables),
                ..Default::default()
            },
        )?;

        // Extract inline dataset fingerprints
        let dataset_fingerprints = inline_datasets
            .iter()
            .map(|(k, ds)| (k.clone(), ds.fingerprint()))
            .collect::<HashMap<_, _>>();

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &dataset_fingerprints)?;
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
                    "Requested variable {var:?}\n requires transforms or signal \
                        expressions that are not yet supported"
                )));
            };

            let value = self
                .get_node_value(
                    Arc::new(task_graph.clone()),
                    node_index,
                    inline_datasets.clone(),
                )
                .await?;

            // Apply row_limit
            let value = if let (Some(row_limit), TaskValue::Table(table)) = (row_limit, &value) {
                if table.num_rows() > row_limit as usize {
                    warnings.push(PreTransformValuesWarning {
                        warning_type: Some(ValuesWarningType::RowLimit(
                            PreTransformRowLimitWarning {
                                datasets: vec![var.0.clone()],
                            },
                        )),
                    });
                    TaskValue::Table(table.head(row_limit as usize))
                } else {
                    value
                }
            } else {
                value
            };

            values.push(value);
        }

        Ok((values, warnings))
    }

    pub async fn pre_transform_extract_request(
        &self,
        request: PreTransformExtractRequest,
    ) -> Result<PreTransformExtractResult> {
        // Extract and deserialize inline datasets
        let inline_pretransform_datasets = request.inline_datasets;
        let inline_datasets = Self::decode_inline_datasets(inline_pretransform_datasets)?;

        // Parse spec
        let spec_string = request.spec;
        let spec: ChartSpec = serde_json::from_str(&spec_string)?;
        let local_tz = request.local_tz;
        let default_input_tz = request.default_input_tz;
        let preserve_interactivity = request.preserve_interactivity;
        let extract_threshold = request.extract_threshold;
        let keep_variables: Vec<ScopedVariable> = request
            .keep_variables
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();

        let (spec, datasets, warnings) = self
            .pre_transform_extract(
                &spec,
                &local_tz,
                &default_input_tz,
                preserve_interactivity,
                extract_threshold as usize,
                inline_datasets,
                keep_variables,
            )
            .await?;

        // Build Response
        let proto_datasets = datasets
            .into_iter()
            .map(|(name, scope, table)| {
                Ok(ProtoPreTransformExtractDataset {
                    name,
                    scope,
                    table: table.to_ipc_bytes()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let response = PreTransformExtractResponse {
            spec: serde_json::to_string(&spec)?,
            datasets: proto_datasets,
            warnings,
        };

        // Build result
        let result = PreTransformExtractResult {
            result: Some(pre_transform_extract_result::Result::Response(response)),
        };

        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn pre_transform_extract(
        &self,
        spec: &ChartSpec,
        local_tz: &str,
        default_input_tz: &Option<String>,
        preserve_interactivity: bool,
        extract_threshold: usize,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        keep_variables: Vec<ScopedVariable>,
    ) -> Result<(
        ChartSpec,
        Vec<PreTransformExtractDataset>,
        Vec<PreTransformExtractWarning>,
    )> {
        let input_spec = spec;

        let (plan, init) = self
            .perform_pre_transform_spec(
                spec,
                local_tz,
                default_input_tz,
                preserve_interactivity,
                inline_datasets,
                keep_variables,
            )
            .await?;

        // Update client spec with server values
        let mut spec = plan.client_spec.clone();
        let mut datasets: Vec<PreTransformExtractDataset> = Vec::new();

        for export_update in init {
            let scope = export_update.scope.clone();
            let name = export_update.name.as_str();
            match export_update.namespace {
                ExportUpdateNamespace::Signal => {
                    // Always inline signal values
                    let signal = spec.get_nested_signal_mut(&scope, name)?;
                    signal.value = MissingNullOrValue::Value(export_update.value.to_json()?);
                }
                ExportUpdateNamespace::Data => {
                    let data = spec.get_nested_data_mut(&scope, name)?;

                    // If the input dataset includes inline values and no transforms,
                    // copy the input JSON directly to avoid the case where round-tripping
                    // through Arrow homogenizes mixed type arrays.
                    // E.g. round tripping may turn [1, "two"] into ["1", "two"]
                    let input_values =
                        input_spec
                            .get_nested_data(&scope, name)
                            .ok()
                            .and_then(|data| {
                                if data.transform.is_empty() {
                                    data.values.clone()
                                } else {
                                    None
                                }
                            });
                    if let Some(input_values) = input_values {
                        // Set inline value
                        data.values = Some(input_values);
                    } else if let TaskValue::Table(table) = export_update.value {
                        if table.num_rows() <= extract_threshold {
                            // Inline small tables
                            data.values = Some(table.to_json()?);
                        } else {
                            // Extract non-small tables
                            datasets.push((export_update.name, export_update.scope, table));
                        }
                    } else {
                        return Err(VegaFusionError::internal(
                            "Expected Data TaskValue to be an Table",
                        ));
                    }
                }
            }
        }

        // Destringify datetime strings in selection store datasets
        destringify_selection_datetimes(&mut spec)?;

        // Build warnings
        let mut warnings: Vec<PreTransformExtractWarning> = Vec::new();

        // Add planner warnings
        for planner_warning in &plan.warnings {
            warnings.push(PreTransformExtractWarning {
                warning_type: Some(pre_transform_extract_warning::WarningType::Planner(
                    PlannerWarning {
                        message: planner_warning.message(),
                    },
                )),
            });
        }

        Ok((spec, datasets, warnings))
    }

    async fn perform_pre_transform_spec(
        &self,
        spec: &ChartSpec,
        local_tz: &str,
        default_input_tz: &Option<String>,
        preserve_interactivity: bool,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        keep_variables: Vec<ScopedVariable>,
    ) -> Result<(SpecPlan, Vec<ExportUpdateArrow>)> {
        // Create spec plan
        let plan = SpecPlan::try_new(
            spec,
            &PlannerConfig::pre_transformed_spec_config(preserve_interactivity, keep_variables),
        )?;
        // println!("pre transform client_spec: {}", serde_json::to_string_pretty(&plan.client_spec).unwrap());
        // println!("pre transform server_spec: {}", serde_json::to_string_pretty(&plan.server_spec).unwrap());
        // println!("pre transform comm plan: {:#?}", plan.comm_plan);

        // Extract inline dataset fingerprints
        let dataset_fingerprints = inline_datasets
            .iter()
            .map(|(k, ds)| (k.clone(), ds.fingerprint()))
            .collect::<HashMap<_, _>>();

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &dataset_fingerprints)
            .unwrap();
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        // Gather values of server-to-client values
        let mut init = Vec::new();
        for var in &plan.comm_plan.server_to_client {
            let node_index = task_graph_mapping
                .get(var)
                .with_context(|| format!("Failed to lookup variable '{var:?}'"))?;
            let value = self
                .get_node_value(
                    Arc::new(task_graph.clone()),
                    node_index,
                    inline_datasets.clone(),
                )
                .await
                .with_context(|| "Failed to get node value")?;

            init.push(ExportUpdateArrow {
                namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
                name: var.0.name.clone(),
                scope: var.1.clone(),
                value,
            });
        }
        Ok((plan, init))
    }

    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }
}

#[async_trait::async_trait]
impl VegaFusionRuntimeTrait for VegaFusionRuntime {
    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<ResponseTaskValue>> {
        // Clone task_graph and task_graph_runtime for use in closure
        let task_graph_runtime = self.clone();
        let response_value_futures: Vec<_> = indices
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
                        .get_node_value(task_graph, node_value_index, inline_datasets.clone())
                        .await?;

                    Ok::<_, VegaFusionError>(ResponseTaskValue {
                        variable: Some(var),
                        scope,
                        value: Some(ProtoTaskValue::try_from(&value).unwrap()),
                    })
                })
            })
            .collect::<Result<Vec<_>>>()?;

        future::try_join_all(response_value_futures).await
    }
}

#[async_recursion]
async fn get_or_compute_node_value(
    task_graph: Arc<TaskGraph>,
    node_index: usize,
    cache: VegaFusionCache,
    inline_datasets: HashMap<String, VegaFusionDataset>,
    conn: Arc<dyn Connection>,
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
                    conn.clone(),
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
                            return Err(VegaFusionError::internal(join_err.to_string()))
                        }
                    };

                    let value = match edge.output {
                        None => value.0,
                        Some(output_index) => value.1.remove(output_index as usize),
                    };
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;

            task.eval(&input_values, &tz_config, inline_datasets, conn)
                .await
        };

        // get or construct from cache

        cache.get_or_try_insert_with(cache_key, fut).await
    }
}
