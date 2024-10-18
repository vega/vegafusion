use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use vegafusion_common::{data::table::VegaFusionTable, error::Result};

use crate::{
    data::dataset::VegaFusionDataset,
    proto::gen::{
        pretransform::{
            PreTransformExtractOpts, PreTransformExtractWarning, PreTransformSpecOpts,
            PreTransformSpecWarning, PreTransformValuesOpts, PreTransformValuesWarning,
        },
        tasks::{NodeValueIndex, ResponseTaskValue, TaskGraph},
    },
    spec::chart::ChartSpec,
    task_graph::task_value::TaskValue,
};

#[derive(Clone)]
pub struct PreTransformExtractTable {
    pub name: String,
    pub scope: Vec<u32>,
    pub table: VegaFusionTable,
}

#[async_trait]
pub trait VegaFusionRuntimeTrait: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<ResponseTaskValue>>;

    async fn pre_transform_spec(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformSpecOpts,
    ) -> Result<(ChartSpec, Vec<PreTransformSpecWarning>)>;

    async fn pre_transform_extract(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformExtractOpts,
    ) -> Result<(
        ChartSpec,
        Vec<PreTransformExtractTable>,
        Vec<PreTransformExtractWarning>,
    )>;

    async fn pre_transform_values(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformValuesOpts,
    ) -> Result<(Vec<TaskValue>, Vec<PreTransformValuesWarning>)>;
}
