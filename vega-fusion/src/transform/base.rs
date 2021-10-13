use crate::error::{Result, VegaFusionError};
use crate::expression::compiler::config::CompilationConfig;
use crate::spec::transform::aggregate::AggregateTransformSpec;
use crate::spec::transform::bin::BinTransformSpec;
use crate::spec::transform::collect::CollectTransformSpec;
use crate::spec::transform::extent::ExtentTransformSpec;
use crate::spec::transform::filter::FilterTransformSpec;
use crate::spec::transform::formula::FormulaTransformSpec;
use crate::spec::transform::TransformSpec;
use crate::transform::aggregate::AggregateTransform;
use crate::transform::bin::BinTransform;
use crate::transform::collect::CollectTransform;
use crate::transform::extent::ExtentTransform;
use crate::transform::filter::FilterTransform;
use crate::transform::formula::FormulaTransform;
use crate::variable::Variable;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

pub trait TransformTrait: Debug + Send + Sync {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)>;

    fn input_vars(&self) -> Vec<Variable> {
        Vec::new()
    }

    fn output_signals(&self) -> Vec<String> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub enum Transform {
    Filter(FilterTransform),
    Extent(ExtentTransform),
    Formula(FormulaTransform),
    Bin(BinTransform),
    Aggregate(AggregateTransform),
    Collect(CollectTransform),
}

impl Deref for Transform {
    type Target = dyn TransformTrait;

    fn deref(&self) -> &Self::Target {
        match self {
            Transform::Filter(tx) => tx,
            Transform::Extent(tx) => tx,
            Transform::Formula(tx) => tx,
            Transform::Bin(tx) => tx,
            Transform::Aggregate(tx) => tx,
            Transform::Collect(tx) => tx,
        }
    }
}

impl From<FilterTransform> for Transform {
    fn from(tx: FilterTransform) -> Self {
        Self::Filter(tx)
    }
}

impl TryFrom<&FilterTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(value: &FilterTransformSpec) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        Ok(Self::Filter(FilterTransform::try_new(value)?))
    }
}

impl From<ExtentTransform> for Transform {
    fn from(tx: ExtentTransform) -> Self {
        Self::Extent(tx)
    }
}

impl TryFrom<&ExtentTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(value: &ExtentTransformSpec) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        Ok(Self::Extent(ExtentTransform::new(value)))
    }
}

impl From<FormulaTransform> for Transform {
    fn from(tx: FormulaTransform) -> Self {
        Self::Formula(tx)
    }
}

impl TryFrom<&FormulaTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(
        value: &FormulaTransformSpec,
    ) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        Ok(Self::Formula(FormulaTransform::try_new(value)?))
    }
}

impl From<BinTransform> for Transform {
    fn from(tx: BinTransform) -> Self {
        Self::Bin(tx)
    }
}

impl TryFrom<&BinTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(value: &BinTransformSpec) -> std::result::Result<Self, Self::Error> {
        Ok(Self::Bin(BinTransform::try_new(value)?))
    }
}

impl From<CollectTransform> for Transform {
    fn from(tx: CollectTransform) -> Self {
        Self::Collect(tx)
    }
}

impl TryFrom<&CollectTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(
        value: &CollectTransformSpec,
    ) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        Ok(Self::Collect(CollectTransform::try_new(value)?))
    }
}

impl From<AggregateTransform> for Transform {
    fn from(tx: AggregateTransform) -> Self {
        Self::Aggregate(tx)
    }
}

impl TryFrom<&AggregateTransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(
        value: &AggregateTransformSpec,
    ) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        Ok(Self::Aggregate(AggregateTransform::new(value)))
    }
}

impl TryFrom<&TransformSpec> for Transform {
    type Error = VegaFusionError;

    fn try_from(value: &TransformSpec) -> std::prelude::rust_2015::Result<Self, Self::Error> {
        match value {
            TransformSpec::Extent(tx_spec) => Self::try_from(tx_spec),
            TransformSpec::Filter(tx_spec) => Self::try_from(tx_spec),
            TransformSpec::Formula(tx_spec) => Self::try_from(tx_spec),
            TransformSpec::Bin(tx_spec) => Self::try_from(tx_spec),
            TransformSpec::Aggregate(tx_spec) => Self::try_from(tx_spec),
            TransformSpec::Collect(tx_spec) => Self::try_from(tx_spec),
        }
    }
}
