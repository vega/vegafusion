use crate::data::util::DataFrameUtils;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::aggregate::make_aggr_expr_for_named_col;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::JoinType;
use datafusion_expr::lit;
use vegafusion_common::column::{relation_col, unescaped_col};
use vegafusion_common::escape::escape_field;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{AggregateOp, JoinAggregate};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;

#[async_trait]
impl TransformTrait for JoinAggregate {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let group_exprs: Vec<_> = self.groupby.iter().map(|c| unescaped_col(c)).collect();
        let schema = dataframe.schema();

        let mut agg_exprs = Vec::new();
        let mut new_col_names = Vec::new();
        for (i, (field, op)) in self.fields.iter().zip(&self.ops).enumerate() {
            let op = AggregateOp::try_from(*op).unwrap();
            let alias = if let Some(alias) = self.aliases.get(i).filter(|a| !a.is_empty()) {
                // Alias is a non-empty string
                alias.clone()
            } else if field.is_empty() {
                op_name(op).to_string()
            } else {
                format!("{}_{}", op_name(op), field)
            };

            let agg_expr = if matches!(op, AggregateOp::Count) {
                // In Vega, the provided column is always ignored if op is 'count'.
                make_aggr_expr_for_named_col(None, &op, schema)?
            } else {
                make_aggr_expr_for_named_col(Some(field.clone()), &op, schema)?
            };

            // Apply alias
            let agg_expr = agg_expr.alias(&alias);

            // Collect new column aliases
            new_col_names.push(alias);

            agg_exprs.push(agg_expr);
        }
        // Perform regular aggregation on clone of input DataFrame
        let agged_df = dataframe
            .clone()
            .aggregate_mixed(group_exprs, agg_exprs)?
            .alias("rhs")?;

        // Join with the input dataframe on the grouping columns
        let mut on = self
            .groupby
            .iter()
            .map(|g| {
                relation_col(&escape_field(g), "lhs").eq(relation_col(&escape_field(g), "rhs"))
            })
            .collect::<Vec<_>>();

        // If there are no groupby columns, use a dummy always-true condition
        // This is needed because DataFusion 48.0 no longer allows empty join conditions
        if on.is_empty() {
            on.push(lit(true));
        }

        let mut final_selections = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if new_col_names.contains(f.name()) {
                    None
                } else {
                    // Add alias to ensure unqualified column name in result
                    Some(relation_col(f.name(), "lhs").alias(f.name()))
                }
            })
            .collect::<Vec<_>>();
        for col in &new_col_names {
            // Add alias to ensure unqualified column name in result
            final_selections.push(relation_col(col, "rhs").alias(col));
        }

        let result = dataframe
            .clone()
            .alias("lhs")?
            .join_on(agged_df, JoinType::Left, on)?
            .select(final_selections)?;

        Ok((result, Vec::new()))
    }
}
