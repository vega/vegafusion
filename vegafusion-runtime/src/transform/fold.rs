use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use async_trait::async_trait;
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::Result;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::Fold;
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
impl TransformTrait for Fold {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        todo!()
        // let field_cols: Vec<_> = self.fields.iter().map(|f| unescape_field(f)).collect();
        // let key_col = unescape_field(
        //     &self
        //         .r#as
        //         .first()
        //         .cloned()
        //         .unwrap_or_else(|| "key".to_string()),
        // );
        // let value_col = unescape_field(
        //     &self
        //         .r#as
        //         .get(1)
        //         .cloned()
        //         .unwrap_or_else(|| "value".to_string()),
        // );
        //
        // let result = dataframe
        //     .fold(field_cols.as_slice(), &value_col, &key_col, Some(ORDER_COL))
        //     .await?;
        // Ok((result, Default::default()))
    }
}
