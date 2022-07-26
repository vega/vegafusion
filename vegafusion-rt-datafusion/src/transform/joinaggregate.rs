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
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{
    avg, col, count, count_distinct, lit, max, min, sum, Expr, JoinType,
};

use crate::expression::compiler::utils::to_numeric;
use async_trait::async_trait;
use datafusion_expr::aggregate_function;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{AggregateOp, JoinAggregate};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for JoinAggregate {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        let mut agg_exprs = Vec::new();
        let mut agg_cols = Vec::new();
        for (i, (field, op)) in self.fields.iter().zip(self.ops.iter()).enumerate() {
            let column = if *op == AggregateOp::Count as i32 {
                // In Vega, the provided column is always ignored if op is 'count'.
                lit(0)
            } else {
                match field.as_str() {
                    "" => {
                        return Err(VegaFusionError::specification(&format!(
                            "Null field is not allowed for {:?} op",
                            op
                        )))
                    }
                    column => col(column),
                }
            };
            let numeric_column = || {
                to_numeric(column.clone(), dataframe.schema()).unwrap_or_else(|_| {
                    panic!("Failed to convert column {:?} to numeric data type", column)
                })
            };

            let op = AggregateOp::from_i32(*op).unwrap();

            let expr = match op {
                AggregateOp::Count => count(column),
                AggregateOp::Mean | AggregateOp::Average => avg(numeric_column()),
                AggregateOp::Min => min(numeric_column()),
                AggregateOp::Max => max(numeric_column()),
                AggregateOp::Sum => sum(numeric_column()),
                AggregateOp::Variance => Expr::AggregateFunction {
                    fun: aggregate_function::AggregateFunction::Variance,
                    distinct: false,
                    args: vec![numeric_column()],
                },
                AggregateOp::Variancep => Expr::AggregateFunction {
                    fun: aggregate_function::AggregateFunction::VariancePop,
                    distinct: false,
                    args: vec![numeric_column()],
                },
                AggregateOp::Stdev => Expr::AggregateFunction {
                    fun: aggregate_function::AggregateFunction::Stddev,
                    distinct: false,
                    args: vec![numeric_column()],
                },
                AggregateOp::Stdevp => Expr::AggregateFunction {
                    fun: aggregate_function::AggregateFunction::StddevPop,
                    distinct: false,
                    args: vec![numeric_column()],
                },
                AggregateOp::Valid => {
                    let valid = Expr::Cast {
                        expr: Box::new(Expr::IsNotNull(Box::new(column))),
                        data_type: DataType::UInt64,
                    };
                    sum(valid)
                }
                AggregateOp::Missing => {
                    let missing = Expr::Cast {
                        expr: Box::new(Expr::IsNull(Box::new(column))),
                        data_type: DataType::UInt64,
                    };
                    sum(missing)
                }
                AggregateOp::Distinct => count_distinct(column),
                _ => {
                    return Err(VegaFusionError::specification(&format!(
                        "Unsupported aggregation op: {:?}",
                        op
                    )))
                }
            };

            // Apply alias
            let expr = if let Some(alias) = self.aliases.get(i).filter(|a| !a.is_empty()) {
                // Alias is a non-empty string
                agg_cols.push(col(alias));
                expr.alias(alias)
            } else {
                let alias = if field.is_empty() {
                    op_name(op).to_string()
                } else {
                    format!("{}_{}", op_name(op), field)
                };
                agg_cols.push(col(&alias));
                expr.alias(&alias)
            };
            agg_exprs.push(expr)
        }

        let group_exprs: Vec<_> = self.groupby.iter().map(|c| col(c)).collect();
        let dataframe = if group_exprs.is_empty() {
            let grouped_dataframe = dataframe
                .aggregate(vec![lit(true).alias("__unit_rhs")], agg_exprs)
                .with_context(|| "Failed to perform joinaggregate transform".to_string())?;

            // Add unit column to join on
            let dataframe =
                dataframe.select(vec![Expr::Wildcard, lit(true).alias("__unit_lhs")])?;

            dataframe.join(
                grouped_dataframe,
                JoinType::Inner,
                &["__unit_rhs"],
                &["__unit_lhs"],
                None,
            )?
        } else {
            let grouped_dataframe = dataframe
                .aggregate(group_exprs, agg_exprs)
                .with_context(|| "Failed to perform joinaggregate transform".to_string())?;

            let left_cols: Vec<_> = self.groupby.iter().map(|f| f.as_str()).collect();

            let groupby_aliases: Vec<String> = self
                .groupby
                .iter()
                .enumerate()
                .map(|(i, _a)| format!("grp_field_{}", i))
                .collect();

            let mut select_exprs = agg_cols.clone();
            select_exprs.extend(
                self.groupby
                    .iter()
                    .zip(&groupby_aliases)
                    .map(|(n, alias)| col(n).alias(alias)),
            );
            let grouped_dataframe = grouped_dataframe.select(select_exprs)?;

            let right_cols: Vec<_> = groupby_aliases.iter().map(|s| s.as_str()).collect();

            dataframe.join(
                grouped_dataframe,
                JoinType::Inner,
                left_cols.as_slice(),
                right_cols.as_slice(),
                None,
            )?
        };

        Ok((dataframe, Vec::new()))
    }
}
