use crate::data::tasks::build_compilation_config;
use crate::expression::compiler::compile;
use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::task::TaskCall;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use crate::data::dataset::VegaFusionDataset;
use crate::task_graph::timezone::RuntimeTzConfig;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::tasks::SignalTask;
use vegafusion_core::task_graph::task::TaskDependencies;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::connection::Connection;

#[async_trait]
impl TaskCall for SignalTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        let config = build_compilation_config(&self.input_vars(), values, tz_config);
        let expression = self.expr.as_ref().unwrap();
        let expr = compile(expression, &config, None)?;
        let value = expr.eval_to_scalar()?;
        let task_value = TaskValue::Scalar(value);
        Ok((task_value, Default::default()))
    }
}
