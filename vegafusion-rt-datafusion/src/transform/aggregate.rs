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
use datafusion::logical_plan::{avg, col, count, count_distinct, lit, max, min, sum, Expr};
use std::collections::HashMap;

use crate::expression::compiler::utils::to_numeric;
use async_trait::async_trait;
use datafusion_expr::{aggregate_function, BuiltInWindowFunction, WindowFunction};
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{Aggregate, AggregateOp};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for Aggregate {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        // DataFusion does not allow repeated (field, op) combinations in an aggregate expression,
        // so if there are duplicates we need to use a projection after the aggregation to alias
        // the desired column
        let mut agg_aliases: HashMap<(Option<String>, i32), String> = HashMap::new();

        // Initialize vec of final projections with the grouping fields
        let mut projections: Vec<_> = self.groupby.iter().map(|f| col(f)).collect();

        for (i, (field, op_code)) in self.fields.iter().zip(self.ops.iter()).enumerate() {
            let op = AggregateOp::from_i32(*op_code).unwrap();

            let column = if *op_code == AggregateOp::Count as i32 {
                // In Vega, the provided column is always ignored if op is 'count'.
                None
            } else {
                match field.as_str() {
                    "" => {
                        return Err(VegaFusionError::specification(&format!(
                            "Null field is not allowed for {:?} op",
                            op
                        )))
                    }
                    column => Some(column.to_string()),
                }
            };

            // Apply alias
            let alias = if let Some(alias) = self.aliases.get(i).filter(|a| !a.is_empty()) {
                // Alias is a non-empty string
                alias.clone()
            } else if field.is_empty() {
                op_name(op).to_string()
            } else {
                format!("{}_{}", op_name(op), field,)
            };

            let key = (column, *op_code);
            if let Some(agg_alias) = agg_aliases.get(&key) {
                // We're already going to preform the aggregation, so alias result
                projections.push(col(agg_alias).alias(&alias));
            } else {
                projections.push(col(&alias));
                agg_aliases.insert(key, alias);
            }
        }

        let mut agg_exprs = Vec::new();

        for ((col_name, op_code), alias) in agg_aliases {
            let op = AggregateOp::from_i32(op_code).unwrap();
            let column = if let Some(col_name) = col_name {
                col(&col_name)
            } else {
                lit(0i32)
            };

            let numeric_column = || {
                to_numeric(column.clone(), dataframe.schema()).unwrap_or_else(|_| {
                    panic!("Failed to convert column {:?} to numeric data type", column)
                })
            };

            let agg_expr = match op {
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
            let agg_expr = agg_expr.alias(&alias);

            agg_exprs.push(agg_expr)
        }

        let dataframe = if !self.groupby.is_empty() {
            //  Add row_number column that we can sort by
            let row_number_expr = Expr::WindowFunction {
                fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
                args: Vec::new(),
                partition_by: Vec::new(),
                order_by: Vec::new(),
                window_frame: None,
            }
            .alias("__row_number");

            // Add min(__row_number) aggregation that we can sort by later
            agg_exprs.push(min(col("__row_number")).alias("__min_row_number"));

            dataframe.select(vec![Expr::Wildcard, row_number_expr])?
        } else {
            dataframe
        };

        let group_exprs: Vec<_> = self.groupby.iter().map(|c| col(c)).collect();
        let mut grouped_dataframe = dataframe
            .aggregate(group_exprs, agg_exprs)
            .with_context(|| "Failed to perform aggregate transform".to_string())?;

        if !self.groupby.is_empty() {
            // Sort groups according to the lowest row number of a value in that group
            let sort_exprs = vec![Expr::Sort {
                expr: Box::new(col("__min_row_number")),
                asc: true,
                nulls_first: false,
            }];
            grouped_dataframe = grouped_dataframe.sort(sort_exprs)?;
        }

        let grouped_dataframe = grouped_dataframe.select(projections)?;
        Ok((grouped_dataframe, Vec::new()))
    }
}
