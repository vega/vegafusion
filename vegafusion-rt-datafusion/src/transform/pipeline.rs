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
use vegafusion_core::proto::gen::tasks::Variable;


impl TransformTrait for TransformPipeline {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let mut result_df = dataframe;
        let mut result_signals: HashMap<String, ScalarValue> = Default::default();
        let mut config = config.clone();

        for tx in &self.transforms {
            let tx_result = tx.call(result_df, &config)?;

            // Update dataframe
            result_df = tx_result.0;

            for (name, val) in tx.output_signals().iter().zip(tx_result.1) {
                result_signals.insert(name.clone(), val.clone());

                // Also add output signals to config scope so they are available to the following
                // transforms
                config.signal_scope.insert(name.clone(), val);
            }
        }

        // Sort result signal value by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = result_signals.into_iter().sorted_by_key(
            |(k, v)| k.clone()
        ).unzip();

        Ok((result_df, signals_values))
    }
}
