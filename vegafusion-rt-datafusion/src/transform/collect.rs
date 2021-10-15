use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use std::sync::Arc;
use crate::expression::compiler::config::CompilationConfig;
use datafusion::scalar::ScalarValue;
use vegafusion_core::error::{Result, ResultWithContext};
use datafusion::logical_plan::{Expr, col};
use vegafusion_core::proto::gen::transforms::{Collect, SortOrder};


impl TransformTrait for Collect {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let sort_exprs: Vec<_> = self
            .fields
            .clone()
            .into_iter()
            .zip(&self.order)
            .map(|(field, order)| Expr::Sort {
                expr: Box::new(col(&field)),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            })
            .collect();

        let result = dataframe
            .sort(sort_exprs)
            .with_context(|| "Collect transform failed".to_string())?;
        Ok((result, Default::default()))
    }
}
