use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use vegafusion_common::error::Result;

use crate::{
    data::dataset::VegaFusionDataset,
    proto::gen::tasks::{NodeValueIndex, ResponseTaskValue, TaskGraph},
};

#[async_trait]
pub trait VegaFusionRuntimeTrait {
    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<ResponseTaskValue>>;
}
