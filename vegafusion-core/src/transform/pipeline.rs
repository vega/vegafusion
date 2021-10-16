use crate::proto::gen::transforms::{TransformPipeline, Transform};
use crate::transform::TransformDependencies;
use std::collections::HashSet;
use itertools::sorted;
use crate::proto::gen::tasks::Variable;
use std::convert::TryFrom;
use crate::spec::transform::TransformSpec;
use crate::error::{VegaFusionError, Result};


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


impl TransformDependencies for TransformPipeline {
    fn input_vars(&self) -> Vec<Variable> {
        let mut vars: HashSet<Variable> = Default::default();
        for tx in &self.transforms {
            for var in tx.input_vars() {
                vars.insert(var);
            }
        }

        sorted(vars).collect()
    }

    fn output_signals(&self) -> Vec<String> {
        let mut signals: HashSet<String> = Default::default();
        for tx in &self.transforms {
            for sig in tx.output_signals() {
                signals.insert(sig);
            }
        }

        sorted(signals).collect()
    }
}