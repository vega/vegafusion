pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod formula;
pub mod pipeline;
pub mod utils;
pub mod task;

use crate::expression::compiler::config::CompilationConfig;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;

use std::sync::Arc;
use vegafusion_core::error::Result;

use vegafusion_core::transform::TransformDependencies;
use vegafusion_core::proto::gen::transforms::Transform;
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
pub trait TransformTrait: TransformDependencies {
    async fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)>;
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

#[async_trait]
impl TransformTrait for Transform {
    async fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        to_transform_trait(self.transform_kind()).call(dataframe, config).await
    }
}
