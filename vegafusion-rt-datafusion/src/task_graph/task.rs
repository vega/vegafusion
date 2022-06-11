/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::task_graph::timezone::RuntimeTzConfig;
use async_trait::async_trait;
use std::collections::HashMap;
use std::convert::TryInto;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::tasks::task::TaskKind;
use vegafusion_core::proto::gen::tasks::Task;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
pub trait TaskCall {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<(TaskValue, Vec<TaskValue>)>;
}

#[async_trait]
impl TaskCall for Task {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        match self.task_kind() {
            TaskKind::Value(value) => Ok((value.try_into()?, Default::default())),
            TaskKind::DataUrl(task) => task.eval(values, tz_config, inline_datasets).await,
            TaskKind::DataValues(task) => task.eval(values, tz_config, inline_datasets).await,
            TaskKind::DataSource(task) => task.eval(values, tz_config, inline_datasets).await,
            TaskKind::Signal(task) => task.eval(values, tz_config, inline_datasets).await,
        }
    }
}
