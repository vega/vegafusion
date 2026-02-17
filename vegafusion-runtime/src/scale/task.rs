#[cfg(feature = "scales")]
#[path = "task_scales.rs"]
mod task_scales;

#[cfg(not(feature = "scales"))]
use {
    crate::task_graph::task::TaskCall,
    crate::task_graph::timezone::RuntimeTzConfig,
    async_trait::async_trait,
    datafusion::prelude::SessionContext,
    std::collections::HashMap,
    std::sync::Arc,
    vegafusion_common::error::{Result, VegaFusionError},
    vegafusion_core::data::dataset::VegaFusionDataset,
    vegafusion_core::proto::gen::tasks::ScaleTask,
    vegafusion_core::task_graph::task_value::TaskValue,
};

#[cfg(not(feature = "scales"))]
#[async_trait]
impl TaskCall for ScaleTask {
    async fn eval(
        &self,
        _values: &[TaskValue],
        _tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        Err(VegaFusionError::internal(
            "Server-side scale evaluation requires the vegafusion-runtime `scales` feature",
        ))
    }
}
