use crate::datafusion::context::make_datafusion_context;
use crate::task_graph::cache::VegaFusionCache;
use crate::task_graph::task::TaskCall;
use crate::task_graph::timezone::RuntimeTzConfig;
use async_recursion::async_recursion;
use cfg_if::cfg_if;
use datafusion::prelude::SessionContext;
use futures_util::{future, FutureExt};
use std::any::Any;
use std::collections::HashMap;
use std::convert::TryInto;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::tasks::inline_dataset::Dataset;
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, InlineDataset, InlineDatasetTable, NodeValueIndex, TaskGraph,
};
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::task_graph::task_value::{NamedTaskValue, TaskValue};

#[cfg(feature = "proto")]
use {
    datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes},
    vegafusion_core::proto::gen::tasks::InlineDatasetPlan,
};

type CacheValue = (TaskValue, Vec<TaskValue>);

#[derive(Clone)]
pub struct VegaFusionRuntime {
    pub cache: VegaFusionCache,
    pub ctx: Arc<SessionContext>,
}

impl VegaFusionRuntime {
    pub fn new(cache: Option<VegaFusionCache>) -> Self {
        Self {
            cache: cache.unwrap_or_else(|| VegaFusionCache::new(Some(32), None)),
            ctx: Arc::new(make_datafusion_context()),
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
            self.ctx.clone(),
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

    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }
}

#[async_trait::async_trait]
impl VegaFusionRuntimeTrait for VegaFusionRuntime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<NamedTaskValue>> {
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
                let variable = match node_value_index.output_index {
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

                    Ok::<_, VegaFusionError>(NamedTaskValue {
                        variable,
                        scope,
                        value,
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
    ctx: Arc<SessionContext>,
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
                let node_fut = get_or_compute_node_value(
                    task_graph.clone(),
                    input_node_index,
                    cloned_cache.clone(),
                    inline_datasets.clone(),
                    ctx.clone(),
                );

                cfg_if! {
                    if #[cfg(target_arch = "wasm32")] {
                        // Add future directly
                        inputs_futures.push(node_fut);
                    } else {
                        // In non-wasm environment, use tokio::spawn for multi-threading
                        inputs_futures.push(tokio::spawn(node_fut));
                    }
                }
            }

            let input_values = futures::future::join_all(inputs_futures).await;

            // Extract the appropriate value from
            let input_values = input_values
                .into_iter()
                .zip(input_edges)
                .map(|(value, edge)| {
                    cfg_if! {
                        if #[cfg(target_arch = "wasm32")] {
                            let mut value = match value {
                                Ok(value) => value,
                                Err(join_err) => {
                                    return Err(join_err)
                                }
                            };
                        } else {
                            // Convert outer JoinHandle error to internal VegaFusionError so we can propagate it.
                            let mut value = match value {
                                Ok(value) => value?,
                                Err(join_err) => {
                                    return Err(VegaFusionError::internal(join_err.to_string()))
                                }
                            };
                        }
                    }

                    let value = match edge.output {
                        None => value.0,
                        Some(output_index) => value.1.remove(output_index as usize),
                    };
                    Ok(value)
                })
                .collect::<Result<Vec<_>>>()?;

            task.eval(&input_values, &tz_config, inline_datasets, ctx)
                .await
        };

        // get or construct from cache
        cache.get_or_try_insert_with(cache_key, fut).await
    }
}

pub async fn decode_inline_datasets(
    inline_pretransform_datasets: Vec<InlineDataset>,
    ctx: &SessionContext,
) -> Result<HashMap<String, VegaFusionDataset>> {
    let mut inline_datasets = HashMap::new();
    for inline_dataset in inline_pretransform_datasets {
        let (name, dataset) = match inline_dataset.dataset.as_ref().unwrap() {
            Dataset::Table(table) => {
                let dataset = VegaFusionDataset::from_table_ipc_bytes(&table.table)?;
                (table.name.clone(), dataset)
            }
            #[cfg(feature = "proto")]
            Dataset::Plan(plan) => {
                let logical_plan = logical_plan_from_bytes(&plan.plan, ctx)?;
                let dataset = VegaFusionDataset::from_plan(logical_plan);
                (plan.name.clone(), dataset)
            }
            #[cfg(not(feature = "proto"))]
            Dataset::Plan(_plan) => {
                return Err(VegaFusionError::internal("proto feature is not enabled"))
            }
        };
        inline_datasets.insert(name, dataset);
    }
    Ok(inline_datasets)
}

pub fn encode_inline_datasets(
    datasets: &HashMap<String, VegaFusionDataset>,
) -> Result<Vec<InlineDataset>> {
    datasets
        .iter()
        .map(|(name, dataset)| {
            let encoded_dataset = match dataset {
                VegaFusionDataset::Table { table, .. } => InlineDataset {
                    dataset: Some(Dataset::Table(InlineDatasetTable {
                        name: name.clone(),
                        table: table.to_ipc_bytes()?,
                    })),
                },
                #[cfg(feature = "proto")]
                VegaFusionDataset::Plan { plan } => InlineDataset {
                    dataset: Some(Dataset::Plan(InlineDatasetPlan {
                        name: name.clone(),
                        plan: logical_plan_to_bytes(plan)?.to_vec(),
                    })),
                },
                #[cfg(not(feature = "proto"))]
                VegaFusionDataset::Plan { .. } => {
                    return Err(VegaFusionError::internal("proto feature is not enabled"))
                }
            };
            Ok(encoded_dataset)
        })
        .collect::<Result<Vec<InlineDataset>>>()
}
