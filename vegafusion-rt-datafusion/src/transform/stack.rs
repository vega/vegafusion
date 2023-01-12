use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::to_numeric;
use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::physical_plan::aggregates;
use datafusion_expr::{
    abs, expr, lit, max, when, AggregateFunction, Expr, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};
use sqlgen::dialect::DialectDisplay;

use crate::expression::escape::{flat_col, unescaped_col};
use std::ops::{Add, Div, Sub};
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::data::ORDER_COL;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{SortOrder, Stack, StackOffset};
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Stack {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let alias0 = self.alias_0.clone().expect("alias0 expected");
        let alias1 = self.alias_1.clone().expect("alias1 expected");

        // Save off input columns
        let input_fields: Vec<_> = dataframe
            .schema_df()
            .fields()
            .iter()
            .map(|f| f.field().name().clone())
            .collect();

        // Build order by vector
        let mut order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| {
                Expr::Sort(expr::Sort {
                    expr: Box::new(unescaped_col(field)),
                    asc: *order == SortOrder::Ascending as i32,
                    nulls_first: *order == SortOrder::Ascending as i32,
                })
            })
            .collect();

        // Order by input row ordering last
        order_by.push(Expr::Sort(expr::Sort {
            expr: Box::new(flat_col(ORDER_COL)),
            asc: true,
            nulls_first: true,
        }));

        // Process according to offset
        let offset = StackOffset::from_i32(self.offset).expect("Failed to convert stack offset");
        let dataframe = match offset {
            StackOffset::Zero => {
                eval_zero_offset(
                    self,
                    dataframe,
                    input_fields.as_slice(),
                    &alias0,
                    &alias1,
                    order_by.as_slice(),
                )
                .await?
            }
            StackOffset::Normalize => {
                eval_normalize_center_offset(
                    self,
                    dataframe,
                    input_fields.as_slice(),
                    &alias0,
                    &alias1,
                    order_by.as_slice(),
                    &offset,
                )
                .await?
            }
            StackOffset::Center => {
                eval_normalize_center_offset(
                    self,
                    dataframe,
                    input_fields.as_slice(),
                    &alias0,
                    &alias1,
                    order_by.as_slice(),
                    &offset,
                )
                .await?
            }
        };

        Ok((dataframe, Default::default()))
    }
}

async fn eval_normalize_center_offset(
    stack: &Stack,
    dataframe: Arc<SqlDataFrame>,
    input_fields: &[String],
    alias0: &str,
    alias1: &str,
    order_by: &[Expr],
    offset: &StackOffset,
) -> Result<Arc<SqlDataFrame>> {
    // Build groupby columns expressions
    let partition_by: Vec<_> = stack
        .groupby
        .iter()
        .map(|group| unescaped_col(group))
        .collect();

    // Cast field to number, replace with 0 when null, take absolute value
    let numeric_field = to_numeric(unescaped_col(&stack.field), &dataframe.schema_df())?;
    let numeric_field =
        when(unescaped_col(&stack.field).is_not_null(), numeric_field).otherwise(lit(0))?;
    let numeric_field = abs(numeric_field);

    let stack_col_name = "__stack";
    let dataframe = dataframe
        .select(vec![Expr::Wildcard, numeric_field.alias(stack_col_name)])
        .await?;

    let total_agg = Expr::AggregateFunction(expr::AggregateFunction {
        fun: AggregateFunction::Sum,
        args: vec![flat_col(stack_col_name)],
        distinct: false,
        filter: None,
    })
    .alias("__total");

    let total_agg_str = total_agg.to_sql_select()?.sql(dataframe.dialect())?;

    // Add __total column with total or total per partition
    let dataframe = if partition_by.is_empty() {
        dataframe
            .chain_query_str(&format!(
                "SELECT * from {parent} CROSS JOIN (SELECT {total_agg_str} from {parent})",
                parent = dataframe.parent_name(),
                total_agg_str = total_agg_str,
            ))
            .await?
    } else {
        let partition_by_strs = partition_by
            .iter()
            .map(|p| Ok(p.to_sql()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;
        let partition_by_csv = partition_by_strs.join(", ");

        dataframe.chain_query_str(&format!(
            "SELECT * from {parent} INNER JOIN (SELECT {partition_by_csv}, {total_agg_str} from {parent} GROUP BY {partition_by_csv}) as __inner USING ({partition_by_csv})",
            parent = dataframe.parent_name(),
            partition_by_csv = partition_by_csv,
            total_agg_str = total_agg_str,
        )).await?
    };

    // Build window function to compute cumulative sum of stack column
    let fun = WindowFunction::AggregateFunction(aggregates::AggregateFunction::Sum);
    let window_expr = Expr::WindowFunction(expr::WindowFunction {
        fun,
        args: vec![flat_col(stack_col_name)],
        partition_by,
        order_by: Vec::from(order_by),
        window_frame: WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::CurrentRow,
        },
    })
    .alias(alias1);

    // Perform selection to add new field value
    let dataframe = dataframe.select(vec![Expr::Wildcard, window_expr]).await?;

    // Build final_selection
    let mut final_selection: Vec<_> = input_fields
        .iter()
        .filter_map(|field| {
            if field == alias0 || field == alias1 {
                None
            } else {
                Some(flat_col(field))
            }
        })
        .collect();

    // Now compute alias1 column by adding numeric field to alias0
    let dataframe = match offset {
        StackOffset::Center => {
            let max_total = max(flat_col("__total")).alias("__max_total");
            let max_total_str = max_total.to_sql_select()?.sql(dataframe.dialect())?;

            let dataframe = dataframe
                .chain_query_str(&format!(
                    "SELECT * from {parent} CROSS JOIN (SELECT {max_total_str} from {parent})",
                    parent = dataframe.parent_name(),
                    max_total_str = max_total_str,
                ))
                .await?;

            let first = flat_col("__max_total").sub(flat_col("__total")).div(lit(2));
            let first_col = flat_col(alias1).add(first);
            let alias1_col = first_col.clone().alias(alias1);
            let alias0_col = first_col.sub(flat_col(stack_col_name)).alias(alias0);
            final_selection.push(alias0_col);
            final_selection.push(alias1_col);

            dataframe
        }
        StackOffset::Normalize => {
            let total_zero = flat_col("__total").eq(lit(0.0));

            let alias0_col = when(total_zero.clone(), lit(0.0))
                .otherwise(
                    flat_col(alias1)
                        .sub(flat_col(stack_col_name))
                        .div(flat_col("__total")),
                )?
                .alias(alias0);

            final_selection.push(alias0_col);

            let alias1_col = when(total_zero, lit(0.0))
                .otherwise(flat_col(alias1).div(flat_col("__total")))?
                .alias(alias1);

            final_selection.push(alias1_col);

            dataframe
        }
        _ => return Err(VegaFusionError::internal("Unexpected stack offset")),
    };

    let dataframe = dataframe.select(final_selection.clone()).await?;
    Ok(dataframe)
}

async fn eval_zero_offset(
    stack: &Stack,
    dataframe: Arc<SqlDataFrame>,
    input_fields: &[String],
    alias0: &str,
    alias1: &str,
    order_by: &[Expr],
) -> Result<Arc<SqlDataFrame>> {
    // Build groupby / partitionby columns
    let partition_by: Vec<_> = stack
        .groupby
        .iter()
        .map(|group| unescaped_col(group))
        .collect();

    // Build window expression
    let fun = WindowFunction::AggregateFunction(aggregates::AggregateFunction::Sum);

    // Cast field to number and replace with 0 when null
    let numeric_field = to_numeric(unescaped_col(&stack.field), &dataframe.schema_df())?;
    let numeric_field =
        when(unescaped_col(&stack.field).is_not_null(), numeric_field).otherwise(lit(0))?;

    // Build window function to compute stacked value
    let window_expr = Expr::WindowFunction(expr::WindowFunction {
        fun,
        args: vec![numeric_field.clone()],
        partition_by,
        order_by: Vec::from(order_by),
        window_frame: WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::CurrentRow,
        },
    })
    .alias(alias1);

    let window_expr_str = window_expr.to_sql_select()?.sql(dataframe.dialect())?;

    // For offset zero, we need to evaluate positive and negative field values separately,
    // then union the results. This is required to make sure stacks do not overlap. Negative
    // values stack in the negative direction and positive values stack in the positive
    // direction.
    let dataframe = dataframe
        .chain_query_str(&format!(
            "(SELECT *, {window_expr_str} from {parent} WHERE {numeric_field} >= 0) UNION ALL \
        (SELECT *, {window_expr_str} from {parent} WHERE {numeric_field} < 0)",
            parent = dataframe.parent_name(),
            window_expr_str = window_expr_str,
            numeric_field = numeric_field.to_sql()?.sql(dataframe.dialect())?
        ))
        .await?;

    // Build final selection
    let mut final_selection: Vec<_> = input_fields
        .iter()
        .filter_map(|field| {
            if field == alias0 || field == alias1 {
                None
            } else {
                Some(flat_col(field))
            }
        })
        .collect();

    // Now compute alias0 column by adding numeric field to alias1
    let alias0_col = flat_col(alias1).sub(numeric_field).alias(alias0);
    final_selection.push(alias0_col);
    final_selection.push(flat_col(alias1));

    let dataframe = dataframe.select(final_selection.clone()).await?;
    Ok(dataframe)
}
