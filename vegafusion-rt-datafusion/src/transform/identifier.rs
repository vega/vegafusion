use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use datafusion_expr::Expr;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Identifier;
use vegafusion_core::task_graph::task_value::TaskValue;
use crate::transform::aggregate::make_row_number_expr;

#[async_trait]
impl TransformTrait for Identifier {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Add row number column with the desired name
        let row_number_expr = make_row_number_expr().alias(&self.r#as);
        let result = dataframe.select(vec![Expr::Wildcard, row_number_expr]).await?;

        Ok((result, Default::default()))
    }
}
