use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use itertools::sorted;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::error::VegaFusionError;
use vegafusion_core::proto::gen::transforms::expression::Transform;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::variable::Variable;

pub struct TransformPipeline {
    transforms: Vec<Transform>,
}

impl TryFrom<&[TransformSpec]> for TransformPipeline {
    type Error = VegaFusionError;

    fn try_from(value: &[TransformSpec]) -> std::result::Result<Self, Self::Error> {
        let transforms: Vec<_> = value
            .iter()
            .map(Transform::try_from)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { transforms })
    }
}

impl TransformPipeline {
    pub fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, HashMap<String, ScalarValue>)> {
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

        Ok((result_df, result_signals))
    }

    pub fn input_vars(&self) -> Vec<Variable> {
        let mut vars: HashSet<Variable> = Default::default();
        for tx in &self.transforms {
            for var in tx.input_vars() {
                vars.insert(var);
            }
        }

        sorted(vars).collect()
    }

    pub fn output_signals(&self) -> Vec<String> {
        let mut signals: HashSet<String> = Default::default();
        for tx in &self.transforms {
            for sig in tx.output_signals() {
                signals.insert(sig);
            }
        }

        sorted(signals).collect()
    }
}
