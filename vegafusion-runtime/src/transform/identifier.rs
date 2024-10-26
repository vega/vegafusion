use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::{expr, Expr, WindowFrame, WindowFunctionDefinition};
use datafusion_functions_window::row_number::RowNumber;
use sqlparser::ast::NullTreatment;
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::Result;
use vegafusion_core::proto::gen::transforms::Identifier;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Identifier {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        // Add row number column with the desired name, sorted by the input order column
        let row_number_expr = Expr::WindowFunction(expr::WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: vec![expr::Sort {
                expr: flat_col(ORDER_COL),
                asc: true,
                nulls_first: false,
            }],
            window_frame: WindowFrame::new(Some(true)),
            null_treatment: Some(NullTreatment::IgnoreNulls),
        })
        .alias(&self.r#as);

        let result = dataframe.select(vec![
            Expr::Wildcard {
                qualifier: None,
                options: WildcardOptions::default(),
            },
            row_number_expr,
        ])?;

        Ok((result, Default::default()))
    }
}
