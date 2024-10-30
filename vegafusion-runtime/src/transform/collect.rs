use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use datafusion_expr::{expr, Expr, WindowFunctionDefinition};
use datafusion_functions_window::row_number::RowNumber;
use sqlparser::ast::NullTreatment;

use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::{Collect, SortOrder};

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_expr::WindowFrame;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Collect {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        // Build vector of sort expressions
        let sort_exprs: Vec<_> = self
            .fields
            .clone()
            .into_iter()
            .zip(&self.order)
            .filter_map(|(field, order)| {
                if dataframe
                    .schema()
                    .inner()
                    .column_with_name(&field)
                    .is_some()
                {
                    let sort_expr = unescaped_col(&field).sort(
                        *order == SortOrder::Ascending as i32,
                        *order == SortOrder::Ascending as i32,
                    );
                    Some(sort_expr)
                } else {
                    None
                }
            })
            .collect();

        // We don't actually sort here, use a row number window function sorted by the sort
        // criteria. This column becomes the new ORDER_COL, which will be sorted at the end of
        // the pipeline.
        let order_col = Expr::WindowFunction(expr::WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
            args: vec![],
            partition_by: vec![],
            order_by: sort_exprs,
            window_frame: WindowFrame::new(Some(true)),
            null_treatment: Some(NullTreatment::IgnoreNulls),
        })
        .alias(ORDER_COL);

        // Build vector of selections
        let mut selections = dataframe
            .schema()
            .inner()
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
            .with_context(|| "Collect transform failed".to_string())?;
        Ok((result, Default::default()))
    }
}
