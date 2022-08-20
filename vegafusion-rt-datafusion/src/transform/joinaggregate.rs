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
use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::aggregate::make_aggr_expr;
use async_trait::async_trait;
use datafusion_expr::{aggregate_function, BuiltInWindowFunction, WindowFunction};
use sqlgen::dialect::DialectDisplay;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{AggregateOp, JoinAggregate};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for JoinAggregate {
    async fn eval_sql(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let group_exprs: Vec<_> = self.groupby.iter().map(|c| col(c)).collect();
        let schema = dataframe.schema_df();

        let mut agg_exprs = Vec::new();
        let mut new_col_exprs = Vec::new();
        for (i, (field, op)) in self.fields.iter().zip(&self.ops).enumerate() {
            let op = AggregateOp::from_i32(*op).unwrap();
            let alias = if let Some(alias) = self.aliases.get(i).filter(|a| !a.is_empty()) {
                // Alias is a non-empty string
                alias.clone()
            } else if field.is_empty() {
                op_name(op).to_string()
            } else {
                format!("{}_{}", op_name(op), field)
            };

            new_col_exprs.push(col(&alias));

            let agg_expr = if matches!(op, AggregateOp::Count) {
                // In Vega, the provided column is always ignored if op is 'count'.
                make_aggr_expr(None, &op, &schema)?
            } else {
                make_aggr_expr(Some(field.clone()), &op, &schema)?
            };

            // Apply alias
            let agg_expr = agg_expr.alias(&alias);

            agg_exprs.push(agg_expr);
        }

        // Build csv str for new columns
        let new_col_strs = new_col_exprs
            .iter()
            .map(|col| Ok(col.to_sql_select()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;
        let new_col_csv = new_col_strs.join(", ");

        // Build csv str of input columns
        let mut input_col_exprs = schema
            .fields()
            .iter()
            .map(|field| col(field.name()))
            .collect::<Vec<_>>();
        let input_col_strs = input_col_exprs
            .iter()
            .map(|c| Ok(c.to_sql_select()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;
        let input_col_csv = input_col_strs.join(", ");

        // Build row_number select expression
        let row_number_expr = Expr::WindowFunction {
            fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: None,
        }
        .alias("__row_number");
        let row_number_str = row_number_expr.to_sql_select()?.sql(dataframe.dialect())?;

        // Perform join aggregation
        let sql_group_expr_strs = group_exprs
            .iter()
            .map(|expr| Ok(expr.to_sql()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;

        let sql_aggr_expr_strs = agg_exprs
            .iter()
            .map(|expr| Ok(expr.to_sql_select()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;
        let aggr_csv = sql_aggr_expr_strs.join(", ");

        let dataframe = if sql_group_expr_strs.is_empty() {
            dataframe.chain_query_str(&format!(
                "select {input_col_csv}, {new_col_csv} \
                from (select *, {row_number_str} from {parent}) \
                CROSS JOIN (select {aggr_csv} from {parent}) as __inner \
                ORDER BY __row_number",
                aggr_csv = aggr_csv,
                parent = dataframe.parent_name(),
                input_col_csv = input_col_csv,
                row_number_str = row_number_str,
                new_col_csv = new_col_csv,
            ))?
        } else {
            let group_by_csv = sql_group_expr_strs.join(", ");
            dataframe.chain_query_str(&format!(
                "select {input_col_csv}, {new_col_csv} \
                from (select *, {row_number_str} from {parent}) \
                LEFT OUTER JOIN (select {aggr_csv}, {group_by_csv} from {parent} group by {group_by_csv}) as __inner USING ({group_by_csv}) \
                ORDER BY __row_number",
                aggr_csv = aggr_csv,
                parent = dataframe.parent_name(),
                input_col_csv = input_col_csv,
                row_number_str = row_number_str,
                new_col_csv = new_col_csv,
                group_by_csv = group_by_csv,
            ))?
        };

        Ok((dataframe, Vec::new()))
    }
}
