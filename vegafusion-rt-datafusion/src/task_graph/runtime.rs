/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use async_recursion::async_recursion;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::task_graph::cache::VegaFusionCache;
use crate::task_graph::task::TaskCall;
use futures_util::{future, FutureExt};
use prost::Message as ProstMessage;
use std::convert::{TryFrom, TryInto};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use vegafusion_core::proto::gen::errors::error::Errorkind;
use vegafusion_core::proto::gen::errors::{Error, TaskGraphValueError};
use vegafusion_core::proto::gen::services::{
    query_request, query_result, QueryRequest, QueryResult,
};
use vegafusion_core::proto::gen::tasks::{
    task::TaskKind, NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueResponse,
    TaskValue as ProtoTaskValue,
};

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

            task.eval(&input_values).await
        };

        // get or construct from cache
        let result = cache.get_or_try_insert_with(cache_key, fut).await;

        result
    }
}
