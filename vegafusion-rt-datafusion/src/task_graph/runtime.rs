use async_recursion::async_recursion;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::task_graph::task::TaskCall;
use std::convert::TryInto;
use std::sync::Arc;
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, TaskGraph, NodeValueIndex
};
use crate::task_graph::cache::VegaFusionCache;

type CacheValue = (TaskValue, Vec<TaskValue>);

#[derive(Clone)]
pub struct TaskGraphRuntime {
    pub cache: VegaFusionCache,
}

impl TaskGraphRuntime {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            cache: VegaFusionCache::new(max_capacity),
        }
    }

    pub async fn get_node_value(
        &self,
        task_graph: Arc<TaskGraph>,
        node_value_index: &NodeValueIndex,
    ) -> Result<TaskValue> {
        let mut node_value =
            get_or_compute_node_value(
                task_graph, node_value_index.node_index as usize, self.cache.clone()
            ).await?;
        Ok(match node_value_index.output_index {
            None => node_value.0,
            Some(output_index) => node_value.1.remove(output_index as usize),
        })
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
                .map(|(mut value, edge)| {
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

            task.eval(&input_values).await
        };

        // get or construct from cache
        let result = cache
            .get_or_try_insert_with(cache_key, fut)
            .await;

        result
    }
}
