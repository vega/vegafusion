pub mod extent;
pub mod filter;
pub mod formula;

use crate::spec::transform::{extent::ExtentTransformSpec, filter::FilterTransformSpec};

use serde::{Deserialize, Serialize};
use std::ops::Deref;
use crate::spec::transform::formula::FormulaTransformSpec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransformSpec {
    Extent(ExtentTransformSpec),
    Filter(FilterTransformSpec),
    Formula(FormulaTransformSpec),
}

impl Deref for TransformSpec {
    type Target = dyn TransformSpecTrait;

    fn deref(&self) -> &Self::Target {
        match self {
            TransformSpec::Extent(t) => t,
            TransformSpec::Filter(t) => t,
            TransformSpec::Formula(t) => t,
        }
    }
}

pub trait TransformSpecTrait {
    fn supported(&self) -> bool {
        true
    }

    fn output_signals(&self) -> Vec<String> {
        Default::default()
    }
}
