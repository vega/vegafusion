use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use vegafusion_common::datatypes::to_boolean;

use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Filter {
    async fn eval(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let filter_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;

        // Cast filter expr to boolean
        let filter_expr = to_boolean(filter_expr, dataframe.schema())?;
        let result = dataframe.filter(filter_expr)?;
        Ok((result, Default::default()))
    }
}
