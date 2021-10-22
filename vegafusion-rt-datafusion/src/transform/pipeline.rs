use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use itertools::{sorted, Itertools};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::error::VegaFusionError;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::proto::gen::transforms::{Transform, TransformPipeline};
use vegafusion_core::transform::TransformDependencies;
use vegafusion_core::proto::gen::tasks::{Variable, VariableNamespace};
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for TransformPipeline {
    async fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let mut result_df = dataframe;
        let mut result_outputs: HashMap<Variable, TaskValue> = Default::default();
        let mut config = config.clone();

        for tx in &self.transforms {
            let tx_result = tx.call(result_df, &config).await?;

            // Update dataframe
            result_df = tx_result.0;

            for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
                result_outputs.insert(var.clone(), val.clone());

                // Also add output signals to config scope so they are available to the following
                // transforms
                match var.ns() {
                    VariableNamespace::Signal => {
                        config.signal_scope.insert(var.name.clone(), val.into_scalar()?);
                    }
                    VariableNamespace::Data => {
                        config.data_scope.insert(var.name.clone(), val.into_table()?);
                    }
                    VariableNamespace::Scale => {
                        unimplemented!()
                    }
                }
            }
        }

        // Sort result signal value by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = result_outputs.into_iter().sorted_by_key(
            |(k, v)| k.clone()
        ).unzip();

        Ok((result_df, signals_values))
    }
}
