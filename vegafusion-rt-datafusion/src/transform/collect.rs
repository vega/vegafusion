use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use datafusion::logical_expr::{Expr, expr};

use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::{Collect, SortOrder};

use crate::expression::escape::unescaped_col;
use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Collect {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let sort_exprs: Vec<_> = self
            .fields
            .clone()
            .into_iter()
            .zip(&self.order)
            .map(|(field, order)| Expr::Sort (expr::Sort {
                expr: Box::new(unescaped_col(&field)),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            }))
            .collect();

        let result = dataframe
            .sort(sort_exprs, None)
            .await
            .with_context(|| "Collect transform failed".to_string())?;
        Ok((result, Default::default()))
    }
}
