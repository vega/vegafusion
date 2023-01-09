use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use datafusion_expr::{BuiltInWindowFunction, Expr, expr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction};
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Identifier;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Identifier {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Add row number column with the desired name
        let row_number_expr = Expr::WindowFunction (expr::WindowFunction {
            fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::CurrentRow
            },
        }).alias(&self.r#as);

        let result = dataframe.select(vec![Expr::Wildcard, row_number_expr]).await?;

        Ok((result, Default::default()))
    }
}
