use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use datafusion_expr::{lit, Expr};
use datafusion_functions_aggregate::median::median_udaf;
use datafusion_functions_aggregate::variance::{var_pop_udaf, var_samp_udaf};
use sqlparser::ast::NullTreatment;
use std::collections::HashMap;

use crate::data::util::DataFrameUtils;
use crate::datafusion::udafs::percentile::{Q1_UDF, Q3_UDF};
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_expr::expr;
use datafusion_functions_aggregate::expr_fn::{avg, count, count_distinct, max, min, sum};
use datafusion_functions_aggregate::stddev::{stddev_pop_udaf, stddev_udaf};
use std::sync::Arc;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::ResultWithContext;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{Aggregate, AggregateOp};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for Aggregate {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let group_exprs: Vec<_> = self
            .groupby
            .iter()
            .filter(|c| {
                dataframe
                    .schema()
                    .inner()
                    .column_with_name(&unescape_field(c))
                    .is_some()
            })
            .map(|c| unescaped_col(c))
            .collect();

        let (mut agg_exprs, projections) = get_agg_and_proj_exprs(self, dataframe.schema())?;

        // Append ordering column to aggregations
        agg_exprs.push(min(flat_col(ORDER_COL)).alias(ORDER_COL));

        // Perform aggregation
        let grouped_dataframe = dataframe.aggregate_mixed(group_exprs, agg_exprs)?;

        // Make final projection
        let grouped_dataframe = grouped_dataframe.select(projections)?;

        Ok((grouped_dataframe, Vec::new()))
    }
}

fn get_agg_and_proj_exprs(tx: &Aggregate, schema: &DFSchema) -> Result<(Vec<Expr>, Vec<Expr>)> {
    // DataFusion does not allow repeated (field, op) combinations in an aggregate expression,
    // so if there are duplicates we need to use a projection after the aggregation to alias
    // the desired column
    let mut agg_aliases: HashMap<(Option<String>, i32), String> = HashMap::new();

    // Initialize vec of final projections with the grouping fields
    let mut projections: Vec<_> = tx.groupby.iter().map(|f| unescaped_col(f)).collect();

    // Prepend ORDER_COL
    projections.insert(0, flat_col(ORDER_COL));

    for (i, (field, op_code)) in tx.fields.iter().zip(tx.ops.iter()).enumerate() {
        let op = AggregateOp::try_from(*op_code).unwrap();

        let column = if *op_code == AggregateOp::Count as i32 {
            // In Vega, the provided column is always ignored if op is 'count'.
            None
        } else {
            match field.as_str() {
                "" => {
                    return Err(VegaFusionError::specification(format!(
                        "Null field is not allowed for {op:?} op"
                    )))
                }
                column => Some(column.to_string()),
            }
        };

        // Apply alias
        let alias = if let Some(alias) = tx.aliases.get(i).filter(|a| !a.is_empty()) {
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
            projections.push(flat_col(agg_alias).alias(&alias));
        } else {
            projections.push(flat_col(&alias));
            agg_aliases.insert(key, alias);
        }
    }

    let mut agg_exprs = Vec::new();

    for ((col_name, op_code), alias) in agg_aliases {
        let op = AggregateOp::try_from(op_code).unwrap();

        let agg_expr = make_aggr_expr_for_named_col(col_name, &op, schema)?;

        // Apply alias
        let agg_expr = agg_expr.alias(alias);

        agg_exprs.push(agg_expr)
    }
    Ok((agg_exprs, projections))
}

pub fn make_aggr_expr_for_named_col(
    col_name: Option<String>,
    op: &AggregateOp,
    schema: &DFSchema,
) -> Result<Expr> {
    let column = if let Some(col_name) = col_name {
        let col_name = unescape_field(&col_name);
        if schema.index_of_column_by_name(None, &col_name).is_none() {
            // No column with specified name, short circuit to return default value
            return if matches!(op, AggregateOp::Sum | AggregateOp::Count) {
                // return zero for sum and count
                Ok(lit(0))
            } else {
                // return NULL for all other operators
                Ok(lit(ScalarValue::Float64(None)))
            };
        } else {
            flat_col(&col_name)
        }
    } else {
        lit(0i32)
    };

    make_agg_expr_for_col_expr(column, op, schema)
}

pub fn make_agg_expr_for_col_expr(
    column: Expr,
    op: &AggregateOp,
    schema: &DFSchema,
) -> Result<Expr> {
    let numeric_column = || -> Result<Expr> {
        to_numeric(column.clone(), schema)
            .with_context(|| format!("Failed to convert column {column:?} to numeric data type"))
    };

    let agg_expr = match op {
        AggregateOp::Count => count(column),
        AggregateOp::Mean | AggregateOp::Average => avg(numeric_column()?),
        AggregateOp::Min => min(column),
        AggregateOp::Max => max(column),
        AggregateOp::Sum => sum(numeric_column()?),
        AggregateOp::Median => Expr::AggregateFunction(expr::AggregateFunction {
            func: median_udaf(),
            distinct: false,
            args: vec![numeric_column()?],
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Variance => Expr::AggregateFunction(expr::AggregateFunction {
            func: var_samp_udaf(),
            distinct: false,
            args: vec![numeric_column()?],
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Variancep => Expr::AggregateFunction(expr::AggregateFunction {
            func: var_pop_udaf(),
            distinct: false,
            args: vec![numeric_column()?],
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Stdev => Expr::AggregateFunction(expr::AggregateFunction {
            func: stddev_udaf(),
            distinct: false,
            args: vec![numeric_column()?],
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Stdevp => Expr::AggregateFunction(expr::AggregateFunction {
            func: stddev_pop_udaf(),
            distinct: false,
            args: vec![numeric_column()?],
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Valid => {
            let valid = Expr::Cast(expr::Cast {
                expr: Box::new(Expr::IsNotNull(Box::new(column))),
                data_type: DataType::Int64,
            });
            sum(valid)
        }
        AggregateOp::Missing => {
            let missing = Expr::Cast(expr::Cast {
                expr: Box::new(Expr::IsNull(Box::new(column))),
                data_type: DataType::Int64,
            });
            sum(missing)
        }
        AggregateOp::Distinct => {
            // Vega counts null as a distinct category but SQL does not
            let missing = Expr::Cast(expr::Cast {
                expr: Box::new(Expr::IsNull(Box::new(column.clone()))),
                data_type: DataType::Int64,
            });
            count_distinct(column) + max(missing)
        }
        AggregateOp::Q1 => Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new((*Q1_UDF).clone()),
            args: vec![numeric_column()?],
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        AggregateOp::Q3 => Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new((*Q3_UDF).clone()),
            args: vec![numeric_column()?],
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        }),
        _ => {
            return Err(VegaFusionError::specification(format!(
                "Unsupported aggregation op: {op:?}"
            )))
        }
    };
    Ok(agg_expr)
}
