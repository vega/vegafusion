use std::ops::Sub;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_expr::{expr, Expr, lit, WindowFrame, WindowFunctionDefinition};
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use datafusion_functions::expr_fn::coalesce;
use datafusion_functions_aggregate::sum::sum_udaf;
use sqlparser::ast::NullTreatment;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::Result;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::{SortOrder, Stack, StackOffset};
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
impl TransformTrait for Stack {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let start_field = self.alias_0.clone().expect("alias0 expected");
        let stop_field = self.alias_1.clone().expect("alias1 expected");

        let field = unescape_field(&self.field);
        let group_by: Vec<_> = self.groupby.iter().map(|f| unescape_field(f)).collect();

        // Build order by vector
        let mut order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| expr::Sort {
                expr: unescaped_col(field),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            })
            .collect();

        // Order by input row ordering last
        order_by.push(expr::Sort {
            expr: flat_col(ORDER_COL),
            asc: true,
            nulls_first: true,
        });

        let offset = StackOffset::try_from(self.offset).expect("Failed to convert stack offset");

        // Build partitioning column expressions
        let partition_by: Vec<_> = group_by.iter().map(|group| flat_col(group)).collect();
        let numeric_field = coalesce(vec![
            to_numeric(flat_col(&field), &dataframe.schema())?,
            lit(0.0),
        ]);

        if let StackOffset::Zero = offset {
            // Build window function to compute stacked value
            let window_expr = Expr::WindowFunction(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                args: vec![numeric_field.clone()],
                partition_by,
                order_by,
                window_frame: WindowFrame::new(Some(true)),
                null_treatment: Some(NullTreatment::IgnoreNulls),
            });

            // Initialize selection with all columns, minus those that conflict with start/stop fields
            let mut select_exprs = dataframe.schema().fields().iter().filter_map(|f| {
                if f.name() == &start_field || f.name() == &stop_field {
                    // Skip fields to be overwritten
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            }).collect::<Vec<_>>();

            // Add stop window expr
            select_exprs.push(window_expr.alias(&stop_field));

            // For offset zero, we need to evaluate positive and negative field values separately,
            // then union the results. This is required to make sure stacks do not overlap. Negative
            // values stack in the negative direction and positive values stack in the positive
            // direction.
            let pos_df = dataframe.clone()
                .filter(numeric_field.clone().gt_eq(lit(0)))?
                .select(select_exprs.clone())?;

            let neg_df = dataframe.clone()
                .filter(numeric_field.clone().lt(lit(0)))?
                .select(select_exprs)?;

            // Union
            let unioned_df = pos_df.union(neg_df)?;

            // Add start window expr
            let result_df = unioned_df.with_column(&start_field, flat_col(&stop_field).sub(numeric_field.clone()))?;

            Ok((result_df, Default::default()))
        } else {
            // Center or Normalized stack modes

            todo!()
        }
    }
}
