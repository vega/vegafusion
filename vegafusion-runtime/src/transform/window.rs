use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, Expr, WindowFrame, WindowFunctionDefinition};
use datafusion_functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use sqlparser::ast::NullTreatment;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowOp,
};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion_expr::test::function_stub::count_udaf;
use datafusion_expr::{BuiltInWindowFunction, WindowFrameBound, WindowFrameUnits};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};
use vegafusion_common::escape::unescape_field;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Window {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
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

        let mut selections: Vec<_> = dataframe
            .schema_df()?
            .fields()
            .iter()
            .map(|f| flat_col(f.name()))
            .collect();

        if order_by.is_empty() {
            // Order by input row if no ordering specified
            order_by.push(Expr::Sort(expr::Sort {
                expr: Box::new(flat_col(ORDER_COL)),
                asc: true,
                nulls_first: true,
            }));
        };

        let partition_by: Vec<_> = self
            .groupby
            .iter()
            .filter(|c| {
                dataframe
                    .schema()
                    .column_with_name(&unescape_field(c))
                    .is_some()
            })
            .map(|group| unescaped_col(group))
            .collect();

        let (start_bound, end_bound) = match &self.frame {
            None => (
                // Unbounded preceding
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                // Current row
                WindowFrameBound::CurrentRow,
            ),
            Some(frame) => (
                WindowFrameBound::Preceding(ScalarValue::UInt64(
                    frame.start.map(|v| (v.unsigned_abs())),
                )),
                WindowFrameBound::Following(ScalarValue::UInt64(frame.end.map(|v| v as u64))),
            ),
        };

        let ignore_peers = self.ignore_peers.unwrap_or(false);

        let units = if ignore_peers {
            WindowFrameUnits::Rows
        } else {
            WindowFrameUnits::Groups
        };
        let window_frame = WindowFrame::new_bounds(units, start_bound, end_bound);

        let schema_df = dataframe.schema_df()?;
        let window_exprs = self
            .ops
            .iter()
            .zip(&self.fields)
            .enumerate()
            .map(|(i, (op, field))| -> Result<Expr> {
                let (window_fn, args) = match op.op.as_ref().unwrap() {
                    window_transform_op::Op::AggregateOp(op) => {
                        let op = AggregateOp::try_from(*op).unwrap();

                        let numeric_field = || -> Result<Expr> {
                            to_numeric(unescaped_col(field), &schema_df).with_context(|| {
                                format!("Failed to convert field {field} to numeric data type")
                            })
                        };

                        use AggregateOp::*;
                        match op {
                            Count => (
                                WindowFunctionDefinition::AggregateUDF(count_udaf()),
                                vec![lit(true)],
                            ),
                            Sum => (
                                WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                                vec![numeric_field()?],
                            ),
                            Mean | Average => (
                                WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                                vec![numeric_field()?],
                            ),
                            Min => (
                                WindowFunctionDefinition::AggregateUDF(min_udaf()),
                                vec![numeric_field()?],
                            ),
                            Max => (
                                WindowFunctionDefinition::AggregateUDF(max_udaf()),
                                vec![numeric_field()?],
                            ),
                            Variance => (
                                WindowFunctionDefinition::AggregateUDF(var_samp_udaf()),
                                vec![numeric_field()?],
                            ),
                            Variancep => (
                                WindowFunctionDefinition::AggregateUDF(var_pop_udaf()),
                                vec![numeric_field()?],
                            ),
                            Stdev => (
                                WindowFunctionDefinition::AggregateUDF(stddev_udaf()),
                                vec![numeric_field()?],
                            ),
                            Stdevp => (
                                WindowFunctionDefinition::AggregateUDF(stddev_pop_udaf()),
                                vec![numeric_field()?],
                            ),
                            // ArrayAgg only available on master right now
                            // Values => (aggregates::AggregateFunction::ArrayAgg, unescaped_col(field)),
                            _ => {
                                return Err(VegaFusionError::compilation(format!(
                                    "Unsupported window aggregate: {op:?}"
                                )))
                            }
                        }
                    }
                    window_transform_op::Op::WindowOp(op) => {
                        let op = WindowOp::try_from(*op).unwrap();
                        let _param = self.params.get(i);

                        let (window_fn, args) = match op {
                            WindowOp::RowNumber => (BuiltInWindowFunction::RowNumber, Vec::new()),
                            WindowOp::Rank => (BuiltInWindowFunction::Rank, Vec::new()),
                            WindowOp::DenseRank => (BuiltInWindowFunction::DenseRank, Vec::new()),
                            WindowOp::PercentileRank => {
                                (BuiltInWindowFunction::PercentRank, vec![])
                            }
                            WindowOp::CumeDist => (BuiltInWindowFunction::CumeDist, vec![]),
                            WindowOp::FirstValue => (
                                BuiltInWindowFunction::FirstValue,
                                vec![unescaped_col(field)],
                            ),
                            WindowOp::LastValue => {
                                (BuiltInWindowFunction::LastValue, vec![unescaped_col(field)])
                            }
                            _ => {
                                return Err(VegaFusionError::compilation(format!(
                                    "Unsupported window function: {op:?}"
                                )))
                            }
                        };
                        (
                            WindowFunctionDefinition::BuiltInWindowFunction(window_fn),
                            args,
                        )
                    }
                };

                let window_expr = Expr::WindowFunction(expr::WindowFunction {
                    fun: window_fn,
                    args,
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                });

                if let Some(alias) = self.aliases.get(i) {
                    Ok(window_expr.alias(alias))
                } else {
                    Ok(window_expr)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // Add window expressions to original selections
        selections.extend(window_exprs);

        let dataframe = dataframe.select(selections).await?;

        Ok((dataframe, Vec::new()))
    }
}
