use std::convert::TryFrom;
use crate::spec::transform::TransformSpec;
use crate::proto::gen::transforms::expression::Transform;
use crate::error::VegaFusionError;
use crate::proto::gen::transforms::{Extent, Filter, Formula, Bin, Aggregate, Collect};

pub mod filter;
pub mod formula;
pub mod collect;
pub mod extent;
pub mod bin;
pub mod aggregate;

impl TryFrom<&TransformSpec> for Transform {
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
