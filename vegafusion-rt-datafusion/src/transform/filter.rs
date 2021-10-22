use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
impl TransformTrait for Filter {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let logical_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;
        let result = dataframe.filter(logical_expr)?;
        Ok((result, Default::default()))
    }
}
