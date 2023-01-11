use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use std::collections::HashSet;

use datafusion::logical_expr::Expr;

use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::aggregate::make_aggr_expr;
use async_trait::async_trait;
use datafusion::common::Column;
use sqlgen::dialect::DialectDisplay;
use std::sync::Arc;

use crate::expression::escape::{flat_col, unescaped_col};
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{AggregateOp, JoinAggregate};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for JoinAggregate {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let group_exprs: Vec<_> = self.groupby.iter().map(|c| unescaped_col(c)).collect();
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

            new_col_exprs.push(flat_col(&alias));

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
        let inner_name = format!("{}_inner", dataframe.parent_name());
        let new_col_names = new_col_exprs
            .iter()
            .map(|col| col.canonical_name())
            .collect::<HashSet<_>>();
        let new_col_strs = new_col_exprs
            .iter()
            .map(|col| {
                let col = Expr::Column(Column {
                    relation: Some(inner_name.to_string()),
                    name: col.canonical_name(),
                })
                .alias(col.canonical_name());
                Ok(col.to_sql_select()?.sql(dataframe.dialect())?)
            })
            .collect::<Result<Vec<_>>>()?;
        let new_col_csv = new_col_strs.join(", ");

        // Build csv str of input columns
        let input_col_exprs = schema
            .fields()
            .iter()
            .filter_map(|field| {
                if new_col_names.contains(field.name()) {
                    None
                } else {
                    Some(flat_col(field.name()))
                }
            })
            .collect::<Vec<_>>();

        let input_col_strs = input_col_exprs
            .iter()
            .map(|c| Ok(c.to_sql_select()?.sql(dataframe.dialect())?))
            .collect::<Result<Vec<_>>>()?;
        let input_col_csv = input_col_strs.join(", ");

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
            dataframe
                .chain_query_str(&format!(
                    "select {input_col_csv}, {new_col_csv} \
                from {parent} \
                CROSS JOIN (select {aggr_csv} from {parent}) as {inner_name}",
                    aggr_csv = aggr_csv,
                    parent = dataframe.parent_name(),
                    input_col_csv = input_col_csv,
                    new_col_csv = new_col_csv,
                    inner_name = inner_name,
                ))
                .await?
        } else {
            let group_by_csv = sql_group_expr_strs.join(", ");
            dataframe.chain_query_str(&format!(
                "select {input_col_csv}, {new_col_csv} \
                from {parent} \
                LEFT OUTER JOIN (select {aggr_csv}, {group_by_csv} from {parent} group by {group_by_csv}) as {inner_name} USING ({group_by_csv})",
                aggr_csv = aggr_csv,
                parent = dataframe.parent_name(),
                input_col_csv = input_col_csv,
                new_col_csv = new_col_csv,
                group_by_csv = group_by_csv,
                inner_name = inner_name,
            )).await?
        };

        Ok((dataframe, Vec::new()))
    }
}
