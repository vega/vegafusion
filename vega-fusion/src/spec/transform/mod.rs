pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod formula;

use crate::spec::transform::{extent::ExtentTransformSpec, filter::FilterTransformSpec};

use crate::spec::transform::aggregate::AggregateTransformSpec;
use crate::spec::transform::bin::BinTransformSpec;
use crate::spec::transform::collect::CollectTransformSpec;
use crate::spec::transform::formula::FormulaTransformSpec;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransformSpec {
    Extent(ExtentTransformSpec),
    Filter(FilterTransformSpec),
    Formula(FormulaTransformSpec),
    Bin(BinTransformSpec),
    Aggregate(AggregateTransformSpec),
    Collect(CollectTransformSpec),
}

impl Deref for TransformSpec {
    type Target = dyn TransformSpecTrait;

    fn deref(&self) -> &Self::Target {
        match self {
            TransformSpec::Extent(t) => t,
            TransformSpec::Filter(t) => t,
            TransformSpec::Formula(t) => t,
            TransformSpec::Bin(t) => t,
            TransformSpec::Aggregate(t) => t,
            TransformSpec::Collect(t) => t,
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
