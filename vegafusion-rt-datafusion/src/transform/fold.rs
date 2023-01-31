use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use crate::sql::dataframe::DataFrame;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_core::data::ORDER_COL;
use vegafusion_core::error::Result;
use vegafusion_core::expression::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::Fold;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Fold {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let field_cols: Vec<_> = self.fields.iter().map(|f| unescape_field(f)).collect();
        let key_col = unescape_field(
            &self
                .r#as
                .get(0)
                .cloned()
                .unwrap_or_else(|| "key".to_string()),
        );
        let value_col = unescape_field(
            &self
                .r#as
                .get(1)
                .cloned()
                .unwrap_or_else(|| "value".to_string()),
        );

        let result = dataframe
            .fold(field_cols.as_slice(), &value_col, &key_col, Some(ORDER_COL))
            .await?;
        Ok((result, Default::default()))
    }
}
