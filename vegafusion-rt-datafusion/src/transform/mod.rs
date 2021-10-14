pub mod filter;
pub mod utils;
pub mod pipeline;

use crate::expression::compiler::config::CompilationConfig;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;

use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::variable::Variable;
use std::ops::Deref;
use vegafusion_core::proto::gen::transforms::expression::Transform;

pub trait TransformTrait {
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

pub fn to_transform_trait(tx: &Transform) -> &dyn TransformTrait {
    match tx {
        Transform::Filter(tx) => tx,
        Transform::Extent(tx) => todo!(),
        Transform::Formula(tx) => todo!(),
        Transform::Bin(tx) => todo!(),
        Transform::Aggregate(tx) => todo!(),
        Transform::Collect(tx) => todo!(),
    }
}

impl TransformTrait for Transform {
    fn call(&self, dataframe: Arc<dyn DataFrame>, config: &CompilationConfig) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        to_transform_trait(self).call(dataframe, config)
    }

    fn input_vars(&self) -> Vec<Variable> {
        to_transform_trait(self).input_vars()
    }

    fn output_signals(&self) -> Vec<String> {
        to_transform_trait(self).output_signals()
    }
}