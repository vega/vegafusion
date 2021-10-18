use crate::task_graph::task::TaskCall;
use vegafusion_core::proto::gen::tasks::{TransformsTask};
use vegafusion_core::task_graph::task_value::TaskValue;
use async_trait::async_trait;
use std::collections::HashMap;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::task_graph::task::TaskDependencies;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use crate::data::table::VegaFusionTableUtils;


#[async_trait]
impl TaskCall for TransformsTask {
    async fn call(&self, values: &[TaskValue]) -> vegafusion_core::error::Result<(TaskValue, Vec<TaskValue>)> {
        // Build compilation config from input_vals
        let mut signal_scope: HashMap<String, ScalarValue> = HashMap::new();
        let mut data_scope: HashMap<String, VegaFusionTable> = HashMap::new();
        for (input_var, input_val) in self.input_vars().iter().zip(values) {
            match input_val {
                TaskValue::Scalar(value) => {
                    signal_scope.insert(input_var.name.clone(), value.clone());
                }
                TaskValue::Table(table) => {
                    data_scope.insert(input_var.name.clone(), table.clone());
                }
            }
        }

        // Get source table from config scope, convert to DataFrame
        let source = data_scope.remove(&self.source).unwrap();

        // Apply transforms, if any
        let pipeline = self.pipeline.as_ref().unwrap();
        let (transformed, signal_values) = if !pipeline.transforms.is_empty() {
            let df = source.to_dataframe()?;

            let (transformed_df, signal_values) = {
                // CompilationConfig is not Send, so use local scope here to make sure it's dropped
                // before the call to await below.
                let config = CompilationConfig {
                    signal_scope,
                    data_scope,
                    ..Default::default()
                };
                pipeline.call(df, &config).await?
            };
            (VegaFusionTable::from_dataframe(transformed_df).await?, signal_values)
        } else {
            (source, Vec::new())
        };

        // Wrap results in TaskValue
        Ok((
            TaskValue::Table(transformed),
            signal_values.into_iter().map(TaskValue::Scalar).collect()
        ))
    }
}
