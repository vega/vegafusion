use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use async_trait::async_trait;
use datafusion_expr::{expr, BuiltInWindowFunction, Expr, WindowFrame, WindowFunctionDefinition};
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::Result;
use vegafusion_core::proto::gen::transforms::Identifier;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Identifier {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        // Add row number column with the desired name, sorted by the input order column
        let row_number_expr = Expr::WindowFunction(expr::WindowFunction {
            fun: WindowFunctionDefinition::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: vec![Expr::Sort(expr::Sort {
                expr: Box::new(flat_col(ORDER_COL)),
                asc: true,
                nulls_first: false,
            })],
            window_frame: WindowFrame::new(Some(true)),
        })
        .alias(&self.r#as);

        let result = dataframe
            .select(vec![Expr::Wildcard { qualifier: None }, row_number_expr])
            .await?;

        Ok((result, Default::default()))
    }
}
