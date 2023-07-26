use std::sync::Arc;
use crate::transform::TransformTrait;
use vegafusion_core::{
    proto::gen::transforms::Facet,
    task_graph::task_value::TaskValue
};
use vegafusion_common::error::Result;
use vegafusion_dataframe::dataframe::DataFrame;
use crate::expression::compiler::config::CompilationConfig;
use async_trait::async_trait;


#[async_trait]
impl TransformTrait for Facet {
    async fn eval(&self, _dataframe: Arc<dyn DataFrame>, _config: &CompilationConfig) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        todo!()
    }
}