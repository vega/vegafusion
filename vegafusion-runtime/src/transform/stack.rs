use crate::data::util::DataFrameUtils;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::JoinType;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::{
    expr, lit, qualified_wildcard, when, Expr, WindowFrame, WindowFunctionDefinition,
};
use datafusion_functions::expr_fn::{abs, coalesce};
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
        let numeric_field = coalesce(vec![
            to_numeric(flat_col(&field), dataframe.schema())?,
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
                Expr::Wildcard {
                    qualifier: None,
                    options: WildcardOptions::default(),
                },
                numeric_field.alias(stack_col_name),
            ])?;

            // Create aggregate for total of stack value
            let total_agg = Expr::AggregateFunction(expr::AggregateFunction {
                func: sum_udaf(),
                args: vec![flat_col(stack_col_name)],
                distinct: false,
                filter: None,
                order_by: None,
                null_treatment: Some(NullTreatment::IgnoreNulls),
            })
            .alias("__total");

            let dataframe = if partition_by.is_empty() {
                // Cross join total aggregation
                dataframe.clone().aggregate(vec![], vec![total_agg])?.join(
                    dataframe,
                    JoinType::Inner,
                    &[],
                    &[],
                    None,
                )?
            } else {
                // Join back total aggregation
                let on_exprs = group_by
                    .iter()
                    .map(|p| relation_col(p, "lhs").eq(relation_col(p, "rhs")))
                    .collect::<Vec<_>>();

                dataframe
                    .clone()
                    .aggregate(partition_by.clone(), vec![total_agg])?
                    .alias("lhs")?
                    .join_on(dataframe.alias("rhs")?, JoinType::Inner, on_exprs)?
                    .select(vec![
                        qualified_wildcard("rhs"),
                        relation_col("__total", "lhs"),
                    ])?
            };

            // Build window function to compute cumulative sum of stack column
            let cumulative_field = "_cumulative";
            let fun = WindowFunctionDefinition::AggregateUDF(sum_udaf());

            let window_expr = Expr::WindowFunction(expr::WindowFunction {
                fun,
                args: vec![flat_col(stack_col_name)],
                partition_by,
                order_by,
                window_frame: WindowFrame::new(Some(true)),
                null_treatment: Some(NullTreatment::IgnoreNulls),
            })
            .alias(cumulative_field);

            // Perform selection to add new field value
            let dataframe = dataframe.select(vec![
                Expr::Wildcard {
                    qualifier: None,
                    options: WildcardOptions::default(),
                },
                window_expr,
            ])?;

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

                    let dataframe = dataframe
                        .clone()
                        .aggregate(vec![], vec![max_total])?
                        .join_on(dataframe, JoinType::Inner, vec![])?;

                    // Add final selections
                    let first = flat_col("__max_total")
                        .sub(flat_col("__total"))
                        .div(lit(2.0));
                    let first_col = flat_col(cumulative_field).add(first);
                    let stop_col = first_col.clone().alias(stop_field);
                    let start_col = first_col.sub(flat_col(stack_col_name)).alias(start_field);
                    final_selection.push(start_col);
                    final_selection.push(stop_col);

                    dataframe
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

            Ok((
                dataframe.select(final_selection.clone())?,
                Default::default(),
            ))
        }
    }
}
