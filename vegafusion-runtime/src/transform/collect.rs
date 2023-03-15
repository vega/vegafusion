use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use datafusion_expr::{expr, Expr};

use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::{Collect, SortOrder};

use async_trait::async_trait;
use datafusion_common::scalar::ScalarValue;
use datafusion_expr::{
    window_function, BuiltInWindowFunction, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Collect {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        // Build vector of sort expressions
        let sort_exprs: Vec<_> = self
            .fields
            .clone()
            .into_iter()
            .zip(&self.order)
            .filter_map(|(field, order)| {
                if dataframe.schema().column_with_name(&field).is_some() {
                    Some(Expr::Sort(expr::Sort {
                        expr: Box::new(unescaped_col(&field)),
                        asc: *order == SortOrder::Ascending as i32,
                        nulls_first: *order == SortOrder::Ascending as i32,
                    }))
                } else {
                    None
                }
            })
            .collect();

        // We don't actually sort here, use a row number window function sorted by the sort
        // criteria. This column becomes the new ORDER_COL, which will be sorted at the end of
        // the pipeline.
        let order_col = Expr::WindowFunction(expr::WindowFunction {
            fun: window_function::WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::RowNumber,
            ),
            args: vec![],
            partition_by: vec![],
            order_by: sort_exprs,
            window_frame: WindowFrame {
                units: WindowFrameUnits::Rows,
                start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                end_bound: WindowFrameBound::CurrentRow,
            },
        })
        .alias(ORDER_COL);

        // Build vector of selections
        let mut selections = dataframe
            .schema()
            .fields
            .iter()
            .filter_map(|field| {
                if field.name() == ORDER_COL {
                    None
                } else {
                    Some(flat_col(field.name()))
                }
            })
            .collect::<Vec<_>>();
        selections.insert(0, order_col);

        let result = dataframe
            .select(selections)
            .await
            .with_context(|| "Collect transform failed".to_string())?;
        Ok((result, Default::default()))
    }
}
