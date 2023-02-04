use crate::data::dataset::VegaFusionDataset;
use crate::task_graph::timezone::RuntimeTzConfig;
use async_trait::async_trait;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::tasks::task::TaskKind;
use vegafusion_core::proto::gen::tasks::Task;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::connection::Connection;

#[async_trait]
pub trait TaskCall {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)>;
}

#[async_trait]
impl TaskCall for Task {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        match self.task_kind() {
            TaskKind::Value(value) => Ok((value.try_into()?, Default::default())),
            TaskKind::DataUrl(task) => task.eval(values, tz_config, inline_datasets, conn).await,
            TaskKind::DataValues(task) => task.eval(values, tz_config, inline_datasets, conn).await,
            TaskKind::DataSource(task) => task.eval(values, tz_config, inline_datasets, conn).await,
            TaskKind::Signal(task) => task.eval(values, tz_config, inline_datasets, conn).await,
        }
    }
}
