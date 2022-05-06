/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::Expr;
use datafusion::prelude::{col, lit};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowOp,
};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::expression::compiler::utils::to_numeric;
use datafusion::physical_plan::aggregates;
use datafusion_expr::{BuiltInWindowFunction, WindowFunction};

#[async_trait]
impl TransformTrait for Window {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        let mut order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| Expr::Sort {
                expr: Box::new(col(field)),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            })
            .collect();

        let mut selections: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.field().name()))
            .collect();

        let dataframe = if order_by.is_empty() {
            //  If no order by fields provided, use the row number
            let row_number_expr = Expr::WindowFunction {
                fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
                args: Vec::new(),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                window_frame: None,
            }
            .alias("__row_number");

            order_by.push(Expr::Sort {
                expr: Box::new(col("__row_number")),
                asc: true,
                nulls_first: true,
            });
            dataframe.select(vec![Expr::Wildcard, row_number_expr])?
        } else {
            dataframe
        };

        let partition_by: Vec<_> = self.groupby.iter().map(|group| col(group)).collect();

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
                            to_numeric(col(field), dataframe.schema()).unwrap_or_else(|_| {
                                panic!("Failed to convert field {} to numeric data type", field)
                            })
                        };

                        use AggregateOp::*;
                        let (agg_fn, arg) = match op {
                            Count => (aggregates::AggregateFunction::Count, lit(true)),
                            Sum => (aggregates::AggregateFunction::Sum, numeric_field()),
                            Mean | Average => (aggregates::AggregateFunction::Avg, numeric_field()),
                            Min => (aggregates::AggregateFunction::Min, numeric_field()),
                            Max => (aggregates::AggregateFunction::Max, numeric_field()),
                            // ArrayAgg only available on master right now
                            // Values => (aggregates::AggregateFunction::ArrayAgg, col(field)),
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
                            WindowOp::FirstValue => {
                                (BuiltInWindowFunction::FirstValue, vec![col(field)])
                            }
                            WindowOp::LastValue => {
                                (BuiltInWindowFunction::LastValue, vec![col(field)])
                            }
                            _ => {
                                panic!("Unsupported window function: {:?}", op)
                            }
                        };
                        (WindowFunction::BuiltInWindowFunction(window_fn), args)
                    }
                };

                let window_expr = Expr::WindowFunction {
                    fun: window_fn,
                    args,
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    window_frame: None,
                };

                if let Some(alias) = self.aliases.get(i) {
                    window_expr.alias(alias)
                } else {
                    window_expr
                }
            })
            .collect();

        // Add window expressions to original selections
        // This will exclude the __row_number column if it was added above.
        selections.extend(window_exprs);

        let dataframe = dataframe.select(selections)?;

        Ok((dataframe, Vec::new()))
    }
}
