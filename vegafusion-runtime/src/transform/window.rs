use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion_common::ScalarValue;
use datafusion_expr::{aggregate_function, expr, lit, Expr};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowOp,
};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion_expr::{
    window_frame, BuiltInWindowFunction, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
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
            .map(|f| flat_col(f.field().name()))
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

        let window_frame = window_frame::WindowFrame {
            units: if ignore_peers {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Groups
            },
            start_bound,
            end_bound,
        };

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
                        let (agg_fn, arg) = match op {
                            Count => (aggregate_function::AggregateFunction::Count, lit(true)),
                            Sum => (aggregate_function::AggregateFunction::Sum, numeric_field()?),
                            Mean | Average => {
                                (aggregate_function::AggregateFunction::Avg, numeric_field()?)
                            }
                            Min => (aggregate_function::AggregateFunction::Min, numeric_field()?),
                            Max => (aggregate_function::AggregateFunction::Max, numeric_field()?),
                            Variance => (
                                aggregate_function::AggregateFunction::Variance,
                                numeric_field()?,
                            ),
                            Variancep => (
                                aggregate_function::AggregateFunction::VariancePop,
                                numeric_field()?,
                            ),
                            Stdev => (
                                aggregate_function::AggregateFunction::Stddev,
                                numeric_field()?,
                            ),
                            Stdevp => (
                                aggregate_function::AggregateFunction::StddevPop,
                                numeric_field()?,
                            ),
                            // ArrayAgg only available on master right now
                            // Values => (aggregates::AggregateFunction::ArrayAgg, unescaped_col(field)),
                            _ => {
                                return Err(VegaFusionError::compilation(format!(
                                    "Unsupported window aggregate: {op:?}"
                                )))
                            }
                        };
                        (WindowFunction::AggregateFunction(agg_fn), vec![arg])
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
                        (WindowFunction::BuiltInWindowFunction(window_fn), args)
                    }
                };

                let window_expr = Expr::WindowFunction(expr::WindowFunction {
                    fun: window_fn,
                    args,
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
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
