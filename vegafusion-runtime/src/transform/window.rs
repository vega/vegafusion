use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion::prelude::DataFrame;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, Expr, WindowFrame, WindowFunctionDefinition};
use datafusion_functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowOp,
};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion_expr::{BuiltInWindowFunction, WindowFrameBound, WindowFrameUnits};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;

use datafusion_functions_window::{cume_dist::CumeDist, rank::Rank, row_number::RowNumber};

use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};
use vegafusion_common::escape::unescape_field;

#[async_trait]
impl TransformTrait for Window {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
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

        let mut selections: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|f| flat_col(f.name()))
            .collect();

        if order_by.is_empty() {
            // Order by input row if no ordering specified
            order_by.push(expr::Sort {
                expr: flat_col(ORDER_COL),
                asc: true,
                nulls_first: true,
            });
        };

        let partition_by: Vec<_> = self
            .groupby
            .iter()
            .filter(|c| {
                dataframe
                    .schema()
                    .inner()
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

        let schema_df = dataframe.schema();
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
                            to_numeric(unescaped_col(field), schema_df).with_context(|| {
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
                            WindowOp::RowNumber => (
                                WindowFunctionDefinition::WindowUDF(Arc::new(
                                    RowNumber::new().into(),
                                )),
                                Vec::new(),
                            ),
                            WindowOp::Rank => (
                                WindowFunctionDefinition::WindowUDF(Arc::new(Rank::basic().into())),
                                Vec::new(),
                            ),
                            WindowOp::DenseRank => (
                                WindowFunctionDefinition::WindowUDF(Arc::new(
                                    Rank::dense_rank().into(),
                                )),
                                Vec::new(),
                            ),
                            WindowOp::PercentileRank => (
                                WindowFunctionDefinition::WindowUDF(Arc::new(
                                    Rank::percent_rank().into(),
                                )),
                                Vec::new(),
                            ),
                            WindowOp::CumeDist => (
                                WindowFunctionDefinition::WindowUDF(Arc::new(
                                    CumeDist::new().into(),
                                )),
                                Vec::new(),
                            ),
                            WindowOp::FirstValue => (
                                WindowFunctionDefinition::BuiltInWindowFunction(
                                    BuiltInWindowFunction::FirstValue,
                                ),
                                vec![unescaped_col(field)],
                            ),
                            WindowOp::LastValue => (
                                WindowFunctionDefinition::BuiltInWindowFunction(
                                    BuiltInWindowFunction::LastValue,
                                ),
                                vec![unescaped_col(field)],
                            ),
                            _ => {
                                return Err(VegaFusionError::compilation(format!(
                                    "Unsupported window function: {op:?}"
                                )))
                            }
                        };
                        (window_fn, args)
                    }
                };

                let window_expr = Expr::WindowFunction(expr::WindowFunction {
                    fun: window_fn,
                    args,
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: None,
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

        let dataframe = dataframe.select(selections)?;

        Ok((dataframe, Vec::new()))
    }
}
