use crate::expression::compiler::config::CompilationConfig;
use crate::transform::aggregate::make_aggr_expr_for_named_col;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_common::column::{flat_col, unescaped_col};

use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{AggregateOp, JoinAggregate};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::aggregate::op_name;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for JoinAggregate {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let group_exprs: Vec<_> = self.groupby.iter().map(|c| unescaped_col(c)).collect();
        let schema = dataframe.schema_df()?;

        let mut agg_exprs = Vec::new();
        let mut new_col_exprs = Vec::new();
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

            new_col_exprs.push(flat_col(&alias));

            let agg_expr = if matches!(op, AggregateOp::Count) {
                // In Vega, the provided column is always ignored if op is 'count'.
                make_aggr_expr_for_named_col(None, &op, &schema)?
            } else {
                make_aggr_expr_for_named_col(Some(field.clone()), &op, &schema)?
            };

            // Apply alias
            let agg_expr = agg_expr.alias(&alias);

            agg_exprs.push(agg_expr);
        }

        let result = dataframe.joinaggregate(group_exprs, agg_exprs).await?;
        Ok((result, Vec::new()))
    }
}
