/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::tasks::Variable;
use crate::proto::gen::transforms::{Transform, TransformPipeline};
use crate::spec::transform::TransformSpec;
use crate::task_graph::task::InputVariable;
use crate::transform::TransformDependencies;
use itertools::sorted;
use std::collections::HashSet;
use std::convert::TryFrom;

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
    fn input_vars(&self) -> Vec<InputVariable> {
        let output_vars: HashSet<_> = self.output_vars().into_iter().collect();

        let mut vars: HashSet<InputVariable> = Default::default();
        for tx in &self.transforms {
            for var in tx.input_vars() {
                // Only include input vars that are not produced elsewhere in the pipeline
                if !output_vars.contains(&var.var) {
                    vars.insert(var);
                }
            }
        }

        sorted(vars).collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        let mut vars: HashSet<Variable> = Default::default();
        for tx in &self.transforms {
            for sig in tx.output_vars() {
                vars.insert(sig);
            }
        }

        sorted(vars).collect()
    }
}
