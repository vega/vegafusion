use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::JoinType;
use datafusion_expr::{
    expr, expr::AggregateFunctionParams, expr::WindowFunctionParams, lit, when, Expr, WindowFrame,
    WindowFunctionDefinition,
};
use datafusion_functions::expr_fn::abs;
use datafusion_functions_aggregate::expr_fn::max;
use datafusion_functions_aggregate::sum::sum_udaf;
use sqlparser::ast::NullTreatment;
use std::ops::{Add, Div, Sub};
use vegafusion_common::column::{flat_col, relation_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::{Result, VegaFusionError};
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

        // Save off input columns
        let input_fields: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

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

        // Convert field to numeric first, then handle nulls with CASE expression
        let numeric_field_expr = to_numeric(flat_col(&field), dataframe.schema())?;
        let numeric_field =
            when(numeric_field_expr.clone().is_null(), lit(0.0)).otherwise(numeric_field_expr)?;

        if let StackOffset::Zero = offset {
            // Build window function to compute stacked value
            let window_expr = Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                params: WindowFunctionParams {
                    args: vec![numeric_field.clone()],
                    partition_by,
                    order_by,
                    window_frame: WindowFrame::new(Some(true)),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                },
            }));

            // Initialize selection with all columns, minus those that conflict with start/stop fields
            let mut select_exprs = dataframe
                .schema()
                .fields()
                .iter()
                .filter_map(|f| {
                    if f.name() == &start_field || f.name() == &stop_field {
                        // Skip fields to be overwritten
                        None
                    } else {
                        Some(flat_col(f.name()))
                    }
                })
                .collect::<Vec<_>>();

            // Add stop window expr
            select_exprs.push(window_expr.alias(&stop_field));

            // For offset zero, we need to evaluate positive and negative field values separately,
            // then union the results. This is required to make sure stacks do not overlap. Negative
            // values stack in the negative direction and positive values stack in the positive
            // direction.
            let pos_df = dataframe
                .clone()
                .filter(numeric_field.clone().gt_eq(lit(0)))?
                .select(select_exprs.clone())?;

            let neg_df = dataframe
                .clone()
                .filter(numeric_field.clone().lt(lit(0)))?
                .select(select_exprs)?;

            // Union
            let unioned_df = pos_df.union(neg_df)?;

            // Add start window expr
            let result_df = unioned_df.with_column(
                &start_field,
                flat_col(&stop_field).sub(numeric_field.clone()),
            )?;

            Ok((result_df, Default::default()))
        } else {
            // Center or Normalized stack modes

            // take absolute value of numeric field
            let numeric_field = abs(numeric_field);

            // Create __stack column with numeric field
            let stack_col_name = "__stack";
            let dataframe = dataframe.select(vec![
                datafusion_expr::expr_fn::wildcard(),
                numeric_field.alias(stack_col_name).into(),
            ])?;

            // Create aggregate for total of stack value
            let total_agg = Expr::AggregateFunction(expr::AggregateFunction {
                func: sum_udaf(),
                params: AggregateFunctionParams {
                    args: vec![flat_col(stack_col_name)],
                    distinct: false,
                    filter: None,
                    order_by: vec![],
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                },
            })
            .alias("__total");

            // Determine the alias for the main dataframe based on whether we have grouping
            let (dataframe, main_alias) = if partition_by.is_empty() {
                // Cross join total aggregation
                // Add dummy join key for cross join since empty join conditions are not allowed
                let dataframe_with_key = dataframe.with_column("__join_key", lit(1))?;
                let agg_df = dataframe_with_key
                    .clone()
                    .aggregate(vec![], vec![total_agg])?
                    .with_column("__join_key", lit(1))?
                    .alias("agg")?;

                // Join on the dummy key
                let joined = dataframe_with_key.alias("orig")?.join_on(
                    agg_df,
                    JoinType::Inner,
                    vec![relation_col("__join_key", "orig").eq(relation_col("__join_key", "agg"))],
                )?;
                (joined, "orig")
            } else {
                // Join back total aggregation
                let on_exprs = group_by
                    .iter()
                    .map(|p| relation_col(p, "lhs").eq(relation_col(p, "rhs")))
                    .collect::<Vec<_>>();

                let lhs_df = dataframe
                    .clone()
                    .aggregate(partition_by.clone(), vec![total_agg])?
                    .alias("lhs")?;
                let rhs_df = dataframe.alias("rhs")?;
                let joined = lhs_df.join_on(rhs_df, JoinType::Inner, on_exprs)?;
                (joined, "rhs")
            };

            // Build window function to compute cumulative sum of stack column
            let cumulative_field = "_cumulative";
            let fun = WindowFunctionDefinition::AggregateUDF(sum_udaf());

            // Update partition_by and order_by to use qualified column references after join
            let partition_by_qualified: Vec<_> = group_by
                .iter()
                .map(|group| relation_col(group, main_alias))
                .collect();

            let order_by_qualified: Vec<_> = self
                .sort_fields
                .iter()
                .zip(&self.sort)
                .map(|(field, order)| expr::Sort {
                    expr: relation_col(&unescape_field(field), main_alias),
                    asc: *order == SortOrder::Ascending as i32,
                    nulls_first: *order == SortOrder::Ascending as i32,
                })
                .chain(std::iter::once(expr::Sort {
                    expr: relation_col(ORDER_COL, main_alias),
                    asc: true,
                    nulls_first: true,
                }))
                .collect();

            let window_expr = Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun,
                params: WindowFunctionParams {
                    args: vec![relation_col(stack_col_name, main_alias)],
                    partition_by: partition_by_qualified,
                    order_by: order_by_qualified,
                    window_frame: WindowFrame::new(Some(true)),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                },
            }))
            .alias(cumulative_field);

            // Perform selection to add new field value
            // After a join, we need to select all columns explicitly to handle aliases properly
            let dataframe = if partition_by.is_empty() {
                // For cross join case, select all columns from orig table
                let mut select_exprs: Vec<Expr> = Vec::new();
                for field in &input_fields {
                    select_exprs.push(relation_col(field, "orig").alias(field));
                }
                // Also select __stack and __total
                select_exprs.push(relation_col("__stack", "orig").alias("__stack"));
                select_exprs.push(relation_col("__total", "agg").alias("__total"));
                // Add the window expression
                select_exprs.push(window_expr.into());
                dataframe.select(select_exprs)?
            } else {
                // For grouped case, we also need to select columns explicitly to ensure proper aliases
                let mut select_exprs: Vec<Expr> = Vec::new();
                for field in &input_fields {
                    select_exprs.push(relation_col(field, main_alias).alias(field));
                }
                // Also select __stack and __total
                select_exprs.push(relation_col("__stack", main_alias).alias("__stack"));
                select_exprs.push(relation_col("__total", "lhs").alias("__total"));
                // Add the window expression
                select_exprs.push(window_expr.into());
                dataframe.select(select_exprs)?
            };

            // Build final_selection
            let mut final_selection: Vec<_> = input_fields
                .iter()
                .filter_map(|field| {
                    if field == &start_field || field == &stop_field {
                        None
                    } else {
                        Some(flat_col(field))
                    }
                })
                .collect();

            // Now compute stop_field column by adding numeric field to start_field
            let dataframe = match offset {
                StackOffset::Center => {
                    let max_total = max(flat_col("__total")).alias("__max_total");

                    // Save original field names
                    let orig_fields: Vec<String> = dataframe
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();

                    // Create a dummy column for joining
                    let mut select_exprs: Vec<Expr> =
                        orig_fields.iter().map(|name| flat_col(name)).collect();
                    select_exprs.push(lit(1).alias("__join_key"));
                    let dataframe_with_key = dataframe.select(select_exprs)?;

                    // Aggregate with the same dummy key
                    let agg_df = dataframe_with_key
                        .clone()
                        .aggregate(vec![flat_col("__join_key")], vec![max_total])?
                        .alias("agg")?;

                    // Join on the dummy key
                    let joined = dataframe_with_key.alias("orig")?.join_on(
                        agg_df,
                        JoinType::Inner,
                        vec![relation_col("__join_key", "orig")
                            .eq(relation_col("__join_key", "agg"))],
                    )?;

                    // Select all original columns plus __max_total (but not __join_key)
                    // Add aliases to ensure unqualified column names in result
                    let mut select_cols: Vec<Expr> = orig_fields
                        .iter()
                        .map(|name| relation_col(name, "orig").alias(name))
                        .collect();
                    select_cols.push(relation_col("__max_total", "agg").alias("__max_total"));

                    let dataframe = joined.select(select_cols)?;

                    // Build the final selection for Center case
                    let mut center_final_selection: Vec<_> = input_fields
                        .iter()
                        .filter_map(|field| {
                            if field == &start_field
                                || field == &stop_field
                                || field.starts_with("__")
                            {
                                None
                            } else {
                                Some(flat_col(field))
                            }
                        })
                        .collect();

                    // Add start and stop columns
                    let first = flat_col("__max_total")
                        .sub(flat_col("__total"))
                        .div(lit(2.0));
                    let first_col = flat_col(cumulative_field).add(first);
                    let stop_col = first_col.clone().alias(stop_field);
                    let start_col = first_col.sub(flat_col(stack_col_name)).alias(start_field);
                    center_final_selection.push(start_col);
                    center_final_selection.push(stop_col);

                    dataframe.select(center_final_selection)?
                }
                StackOffset::Normalize => {
                    let total_zero = flat_col("__total").eq(lit(0.0));

                    let start_col = when(total_zero.clone(), lit(0.0))
                        .otherwise(
                            flat_col(cumulative_field)
                                .sub(flat_col(stack_col_name))
                                .div(flat_col("__total")),
                        )?
                        .alias(start_field);

                    final_selection.push(start_col);

                    let stop_col = when(total_zero, lit(0.0))
                        .otherwise(flat_col(cumulative_field).div(flat_col("__total")))?
                        .alias(stop_field);

                    final_selection.push(stop_col);

                    dataframe
                }
                _ => return Err(VegaFusionError::internal("Unexpected stack mode")),
            };

            match offset {
                StackOffset::Center => Ok((dataframe, Default::default())),
                _ => Ok((dataframe.select(final_selection)?, Default::default())),
            }
        }
    }
}
