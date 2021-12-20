use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;

use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
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
        // Save off initial columns and select them below to filter out any intermediary columns
        // that the expression may produce
        let col_names: Vec<_> = dataframe.schema().fields().iter().map(|field| field.name().as_str()).collect();
        let result = dataframe.filter(
            logical_expr
        )?.select_columns(
            &col_names
        )?;

        Ok((result, Default::default()))
    }
}
