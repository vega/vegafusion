use vegafusion_core::{
    data::dataset::VegaFusionDataset,
    proto::gen::{
        services::{
            query_request, query_result, vega_fusion_runtime_client::VegaFusionRuntimeClient,
            GetCapabilitiesRequest, QueryRequest,
        },
        tasks::{NodeValueIndex, TaskGraph, TaskGraphValueRequest},
    },
    runtime::{MergedCapabilities, VegaFusionRuntimeTrait},
    task_graph::task_value::NamedTaskValue,
};

use crate::task_graph::runtime::encode_inline_datasets;
use async_mutex::Mutex;
use async_trait::async_trait;
use std::collections::HashMap;
use std::{any::Any, sync::Arc};
use vegafusion_common::error::{Result, VegaFusionError};

#[derive(Clone)]
pub struct GrpcVegaFusionRuntime {
    client: Arc<Mutex<VegaFusionRuntimeClient<tonic::transport::Channel>>>,
    capabilities: MergedCapabilities,
}

#[async_trait]
impl VegaFusionRuntimeTrait for GrpcVegaFusionRuntime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn planner_capabilities(&self) -> MergedCapabilities {
        self.capabilities.clone()
    }

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<NamedTaskValue>> {
        let inline_datasets = encode_inline_datasets(inline_datasets)?;
        let request = QueryRequest {
            request: Some(query_request::Request::TaskGraphValues(
                TaskGraphValueRequest {
                    task_graph: Some(task_graph.as_ref().clone()),
                    indices: indices.to_vec(),
                    inline_datasets,
                },
            )),
        };

        let mut locked_client = self.client.lock().await;
        let response = locked_client
            .task_graph_query(request)
            .await
            .map_err(|e| VegaFusionError::internal(e.to_string()))?;
        match response.into_inner().response.unwrap() {
            query_result::Response::TaskGraphValues(task_graph_values) => Ok(task_graph_values
                .response_values
                .into_iter()
                .map(|v| v.into())
                .collect::<Vec<_>>()),
            _ => Err(VegaFusionError::internal(
                "Invalid response type".to_string(),
            )),
        }
    }
}

impl GrpcVegaFusionRuntime {
    pub async fn try_new(channel: tonic::transport::Channel) -> Result<Self> {
        let mut client = VegaFusionRuntimeClient::new(channel);

        // Fetch capabilities from the server at construction time
        let caps_response = client
            .get_capabilities(GetCapabilitiesRequest {})
            .await
            .map_err(|e| VegaFusionError::internal(format!("Failed to get capabilities: {e}")))?;
        let caps = caps_response.into_inner().capabilities.unwrap_or_default();
        let capabilities = MergedCapabilities::from_resolver_capabilities(&[caps]);

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            capabilities,
        })
    }
}
