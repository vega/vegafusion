use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{expr, Expr};
use datafusion::prelude::lit;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowOp,
};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::expression::compiler::utils::to_numeric;
use crate::expression::escape::{flat_col, unescaped_col};
use crate::sql::dataframe::SqlDataFrame;
use datafusion::physical_plan::aggregates;
use datafusion_expr::{
    window_frame, BuiltInWindowFunction, WindowFrameBound, WindowFrameUnits, WindowFunction,
};
use vegafusion_core::data::ORDER_COL;

#[async_trait]
impl TransformTrait for Window {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
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
            .schema_df()
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

        let window_exprs: Vec<_> = self
            .ops
            .iter()
            .zip(&self.fields)
            .enumerate()
            .map(|(i, (op, field))| {
                let (window_fn, args) = match op.op.as_ref().unwrap() {
                    window_transform_op::Op::AggregateOp(op) => {
                        let op = AggregateOp::from_i32(*op).unwrap();

                        let numeric_field = || {
                            to_numeric(unescaped_col(field), &dataframe.schema_df()).unwrap_or_else(
                                |_| {
                                    panic!("Failed to convert field {} to numeric data type", field)
                                },
                            )
                        };

                        use AggregateOp::*;
                        let (agg_fn, arg) = match op {
                            Count => (aggregates::AggregateFunction::Count, lit(true)),
                            Sum => (aggregates::AggregateFunction::Sum, numeric_field()),
                            Mean | Average => (aggregates::AggregateFunction::Avg, numeric_field()),
                            Min => (aggregates::AggregateFunction::Min, numeric_field()),
                            Max => (aggregates::AggregateFunction::Max, numeric_field()),
                            Variance => (aggregates::AggregateFunction::Variance, numeric_field()),
                            Variancep => {
                                (aggregates::AggregateFunction::VariancePop, numeric_field())
                            }
                            Stdev => (aggregates::AggregateFunction::Stddev, numeric_field()),
                            Stdevp => (aggregates::AggregateFunction::StddevPop, numeric_field()),
                            // ArrayAgg only available on master right now
                            // Values => (aggregates::AggregateFunction::ArrayAgg, unescaped_col(field)),
                            _ => {
                                panic!("Unsupported window aggregate: {:?}", op)
                            }
                        };
                        (WindowFunction::AggregateFunction(agg_fn), vec![arg])
                    }
                    window_transform_op::Op::WindowOp(op) => {
                        let op = WindowOp::from_i32(*op).unwrap();
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
                                panic!("Unsupported window function: {:?}", op)
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
                    window_expr.alias(alias)
                } else {
                    window_expr
                }
            })
            .collect();

        // Add window expressions to original selections
        selections.extend(window_exprs);

        let dataframe = dataframe.select(selections).await?;

        Ok((dataframe, Vec::new()))
    }
}
