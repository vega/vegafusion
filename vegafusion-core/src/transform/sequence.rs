use crate::error::Result;
use crate::expression::parser::parse;
use crate::proto::gen::transforms::Sequence;
use crate::spec::transform::sequence::SequenceTransformSpec;
use crate::spec::values::{NumberOrSignalSpec, SignalExpressionSpec};
use crate::transform::TransformDependencies;
use itertools::sorted;
use std::collections::HashSet;

use crate::task_graph::task::InputVariable;

impl Sequence {
    pub fn try_new(spec: &SequenceTransformSpec) -> Result<Self> {
        let start = match &spec.start {
            NumberOrSignalSpec::Number(v) => parse(&v.to_string())?,
            NumberOrSignalSpec::Signal(SignalExpressionSpec { signal }) => parse(signal.as_str())?,
        };
        let stop = match &spec.stop {
            NumberOrSignalSpec::Number(v) => parse(&v.to_string())?,
            NumberOrSignalSpec::Signal(SignalExpressionSpec { signal }) => parse(signal.as_str())?,
        };

        let step = match &spec.step {
            None => None,
            Some(step) => match step {
                NumberOrSignalSpec::Number(v) => Some(parse(&v.to_string())?),
                NumberOrSignalSpec::Signal(SignalExpressionSpec { signal }) => {
                    Some(parse(signal.as_str())?)
                }
            },
        };

        Ok(Self {
            start: Some(start),
            stop: Some(stop),
            step,
            r#as: Some(spec.as_()),
        })
    }
}

impl TransformDependencies for Sequence {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut input_vars: HashSet<InputVariable> = HashSet::new();
        input_vars.extend(self.start.as_ref().unwrap().input_vars());
        input_vars.extend(self.stop.as_ref().unwrap().input_vars());
        if let Some(step) = &self.step {
            input_vars.extend(step.input_vars());
        }
        sorted(input_vars).collect()
    }
}
