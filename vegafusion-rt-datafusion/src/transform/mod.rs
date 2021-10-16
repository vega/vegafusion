pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod formula;
pub mod pipeline;
pub mod utils;

use crate::expression::compiler::config::CompilationConfig;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;

use std::sync::Arc;
use vegafusion_core::error::Result;

use vegafusion_core::transform::TransformDependencies;
use vegafusion_core::proto::gen::transforms::Transform;
use vegafusion_core::proto::gen::transforms::transform::TransformKind;

pub trait TransformTrait: TransformDependencies {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)>;
}

pub fn to_transform_trait(tx: &TransformKind) -> &dyn TransformTrait {
    match tx {
        TransformKind::Filter(tx) => tx,
        TransformKind::Extent(tx) => tx,
        TransformKind::Formula(tx) => tx,
        TransformKind::Bin(tx) => tx,
        TransformKind::Aggregate(tx) => tx,
        TransformKind::Collect(tx) => tx,
    }
}

impl TransformTrait for Transform {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        to_transform_trait(self.transform_kind()).call(dataframe, config)
    }
}
