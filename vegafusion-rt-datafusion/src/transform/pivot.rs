use crate::expression::compiler::config::CompilationConfig;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Pivot;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Pivot {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        todo!()
    }
}
