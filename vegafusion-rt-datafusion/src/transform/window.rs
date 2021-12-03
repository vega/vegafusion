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

use datafusion::physical_plan::aggregates;
use datafusion::physical_plan::window_functions::{BuiltInWindowFunction, WindowFunction};

#[async_trait]
impl TransformTrait for Window {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| Expr::Sort {
                expr: Box::new(col(field)),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            })
            .collect();

        let partition_by: Vec<_> = self.groupby.iter().map(|group| col(group)).collect();

        // Make window function
        // let builtin_window_fn = BuiltInWindowFunction::RowNumber;
        // let window_fn = WindowFunction::BuiltInWindowFunction(builtin_window_fn);

        let window_exprs: Vec<_> = self
            .ops
            .iter()
            .zip(&self.fields)
            .enumerate()
            .map(|(i, (op, field))| {
                let (window_fn, args) = match op.op.as_ref().unwrap() {
                    window_transform_op::Op::AggregateOp(op) => {
                        let op = AggregateOp::from_i32(*op).unwrap();

                        use AggregateOp::*;
                        let (agg_fn, arg) = match op {
                            Count => (aggregates::AggregateFunction::Count, lit(true)),
                            Sum => (aggregates::AggregateFunction::Sum, col(field)),
                            Mean | Average => (aggregates::AggregateFunction::Avg, col(field)),
                            Min => (aggregates::AggregateFunction::Min, col(field)),
                            Max => (aggregates::AggregateFunction::Max, col(field)),
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
                            WindowOp::Rank => (BuiltInWindowFunction::Rank, vec![]),
                            WindowOp::DenseRank => (BuiltInWindowFunction::DenseRank, vec![]),
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

        // let window_fn = WindowFunction::AggregateFunction(
        //     aggregates::AggregateFunction::Count
        // );

        // Make frame
        // let window_frame = WindowFrame {
        //     units: WindowFrameUnits::Rows,
        //     start_bound: WindowFrameBound::Preceding(None),
        //     end_bound: WindowFrameBound::CurrentRow,
        // };

        //
        //
        // let window_expr = Expr::WindowFunction {
        //     fun: window_fn,
        //     args: vec![lit(true)],
        //     partition_by,
        //     order_by,
        //     window_frame: None,
        // };

        // // Apply alias
        // let window_expr = if let Some(alias) = self.aliases.get(0) {
        //     println!("self.aliases.get(0): {:?}", alias);
        //     window_expr.alias(alias)
        // } else {
        //     window_expr
        // };

        // println!("{:#?}", window_expr);

        // Apply window in select expression
        let mut selections: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.field().name()))
            .collect();
        selections.extend(window_exprs);

        // let dataframe = dataframe.select(vec![window_expr])?;
        let dataframe = dataframe.select(selections)?;

        Ok((dataframe, Vec::new()))
    }
}
