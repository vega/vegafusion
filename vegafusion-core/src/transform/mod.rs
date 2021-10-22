use crate::error::VegaFusionError;
use crate::proto::gen::transforms::transform::TransformKind;
use crate::proto::gen::transforms::Transform;
use crate::proto::gen::transforms::{Aggregate, Bin, Collect, Extent, Filter, Formula};
use crate::spec::transform::TransformSpec;
use std::convert::TryFrom;
use crate::proto::gen::tasks::Variable;
use crate::task_graph::task::InputVariable;


pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod formula;
pub mod pipeline;
pub mod task;

impl TryFrom<&TransformSpec> for TransformKind {
    type Error = VegaFusionError;

    fn try_from(value: &TransformSpec) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            TransformSpec::Extent(tx_spec) => Self::Extent(Extent::new(tx_spec)),
            TransformSpec::Filter(tx_spec) => Self::Filter(Filter::try_new(tx_spec)?),
            TransformSpec::Formula(tx_spec) => Self::Formula(Formula::try_new(tx_spec)?),
            TransformSpec::Bin(tx_spec) => Self::Bin(Bin::try_new(tx_spec)?),
            TransformSpec::Aggregate(tx_spec) => Self::Aggregate(Aggregate::new(tx_spec)),
            TransformSpec::Collect(tx_spec) => Self::Collect(Collect::try_new(tx_spec)?),
        })
    }
}

impl TryFrom<&TransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(value: &TransformSpec) -> Result<Self, Self::Error> {
        Ok(Self { transform_kind: Some(TransformKind::try_from(value)?) })
    }
}


impl TransformKind {
    pub fn as_dependencies_trait(&self) -> &dyn TransformDependencies {
        match self {
            TransformKind::Filter(tx) => tx,
            TransformKind::Extent(tx) => tx,
            TransformKind::Formula(tx) => tx,
            TransformKind::Bin(tx) => tx,
            TransformKind::Aggregate(tx) => tx,
            TransformKind::Collect(tx) => tx,
        }
    }
}

impl Transform {
    pub fn transform_kind(&self) -> &TransformKind {
        self.transform_kind.as_ref().unwrap()
    }
}

pub trait TransformDependencies: Send + Sync {
    fn input_vars(&self) -> Vec<InputVariable> {
        Vec::new()
    }

    fn output_vars(&self) -> Vec<Variable> {
        Vec::new()
    }
}

impl TransformDependencies for Transform {
    fn input_vars(&self) -> Vec<InputVariable> {
        self.transform_kind().as_dependencies_trait().input_vars()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.transform_kind().as_dependencies_trait().output_vars()
    }
}

