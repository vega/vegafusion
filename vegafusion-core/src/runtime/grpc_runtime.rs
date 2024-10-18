use crate::{
    data::dataset::VegaFusionDataset,
    proto::{
        gen::services::vega_fusion_runtime_client::VegaFusionRuntimeClient,
        gen::{
            pretransform::{
                PreTransformExtractOpts, PreTransformExtractRequest, PreTransformExtractWarning,
                PreTransformSpecOpts, PreTransformSpecRequest, PreTransformSpecWarning,
                PreTransformValuesOpts, PreTransformValuesRequest, PreTransformValuesWarning,
                PreTransformVariable,
            },
            services::{
                pre_transform_extract_result, pre_transform_spec_result,
                pre_transform_values_result, query_request, query_result, QueryRequest,
            },
            tasks::{
                InlineDataset, NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueRequest,
            },
        },
    },
    spec::chart::ChartSpec,
    task_graph::{graph::ScopedVariable, task_value::TaskValue},
};

use async_mutex::Mutex;
use async_trait::async_trait;
use std::collections::HashMap;
use std::{any::Any, sync::Arc};
use vegafusion_common::{
    data::table::VegaFusionTable,
    error::{Result, VegaFusionError},
};

use super::{runtime::PreTransformExtractTable, VegaFusionRuntimeTrait};

#[derive(Clone)]
pub struct GrpcVegaFusionRuntime {
    client: Arc<Mutex<VegaFusionRuntimeClient<tonic::transport::Channel>>>,
}

#[async_trait]
impl VegaFusionRuntimeTrait for GrpcVegaFusionRuntime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        _inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<ResponseTaskValue>> {
        let request = QueryRequest {
            request: Some(query_request::Request::TaskGraphValues(
                TaskGraphValueRequest {
                    task_graph: Some(task_graph.as_ref().clone()),
                    indices: indices.to_vec(),
                    inline_datasets: vec![], // inline_datasets.clone(),
                },
            )),
        };

        let mut locked_client = self.client.lock().await;
        let response = locked_client
            .task_graph_query(request)
            .await
            .map_err(|e| VegaFusionError::internal(e.to_string()))?;
        match response.into_inner().response.unwrap() {
            query_result::Response::TaskGraphValues(task_graph_values) => {
                Ok(task_graph_values.response_values)
            }
            _ => Err(VegaFusionError::internal(
                "Invalid response type".to_string(),
            )),
        }
    }

    async fn pre_transform_spec(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformSpecOpts,
    ) -> Result<(ChartSpec, Vec<PreTransformSpecWarning>)> {
        let inline_datasets = encode_inline_datasets(&inline_datasets)?;

        let request = PreTransformSpecRequest {
            spec: serde_json::to_string(spec)?,
            inline_datasets,
            opts: Some(options.clone()),
        };
        let mut locked_client = self.client.lock().await;
        let response = locked_client
            .pre_transform_spec(request)
            .await
            .map_err(|e| VegaFusionError::internal(e.to_string()))?;

        match response.into_inner().result.unwrap() {
            pre_transform_spec_result::Result::Response(response) => {
                Ok((serde_json::from_str(&response.spec)?, response.warnings))
            }
            _ => Err(VegaFusionError::internal(
                "Invalid grpc response type".to_string(),
            )),
        }
    }

    async fn pre_transform_extract(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformExtractOpts,
    ) -> Result<(
        ChartSpec,
        Vec<PreTransformExtractTable>,
        Vec<PreTransformExtractWarning>,
    )> {
        let inline_datasets = encode_inline_datasets(&inline_datasets)?;

        let request = PreTransformExtractRequest {
            spec: serde_json::to_string(spec)?,
            inline_datasets,
            opts: Some(options.clone()),
        };

        let mut locked_client = self.client.lock().await;
        let response = locked_client
            .pre_transform_extract(request)
            .await
            .map_err(|e| VegaFusionError::internal(e.to_string()))?;

        match response.into_inner().result.unwrap() {
            pre_transform_extract_result::Result::Response(response) => {
                let spec: ChartSpec = serde_json::from_str(&response.spec)?;
                let datasets = response
                    .datasets
                    .into_iter()
                    .map(|ds| {
                        Ok(PreTransformExtractTable {
                            name: ds.name,
                            scope: ds.scope,
                            table: VegaFusionTable::from_ipc_bytes(&ds.table)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((spec, datasets, response.warnings))
            }
            _ => Err(VegaFusionError::internal(
                "Invalid grpc response type".to_string(),
            )),
        }
    }

    async fn pre_transform_values(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformValuesOpts,
    ) -> Result<(Vec<TaskValue>, Vec<PreTransformValuesWarning>)> {
        let inline_datasets = encode_inline_datasets(&inline_datasets)?;

        let request = PreTransformValuesRequest {
            spec: serde_json::to_string(spec)?,
            inline_datasets,
            opts: Some(options.clone()),
        };

        let mut locked_client = self.client.lock().await;
        let response = locked_client
            .pre_transform_values(request)
            .await
            .map_err(|e| VegaFusionError::internal(e.to_string()))?;

        match response.into_inner().result.unwrap() {
            pre_transform_values_result::Result::Response(response) => {
                let values = response
                    .values
                    .into_iter()
                    .map(|v| TaskValue::try_from(&v.value.unwrap()))
                    .collect::<Result<Vec<_>>>()?;
                Ok((values, response.warnings))
            }
            _ => Err(VegaFusionError::internal(
                "Invalid grpc response type".to_string(),
            )),
        }
    }
}

impl GrpcVegaFusionRuntime {
    pub async fn try_new(channel: tonic::transport::Channel) -> Result<Self> {
        let client = VegaFusionRuntimeClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

fn encode_inline_datasets(
    datasets: &HashMap<String, VegaFusionDataset>,
) -> Result<Vec<InlineDataset>> {
    datasets
        .into_iter()
        .map(|(name, dataset)| {
            let VegaFusionDataset::Table { table, hash: _ } = dataset else {
                return Err(VegaFusionError::internal(
                    "grpc runtime suppors Arrow tables only, not general Datasets".to_string(),
                ));
            };
            Ok(InlineDataset {
                name: name.clone(),
                table: table.to_ipc_bytes()?,
            })
        })
        .collect::<Result<Vec<InlineDataset>>>()
}
