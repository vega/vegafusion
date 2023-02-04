use crate::expression::compiler::config::CompilationConfig;

use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use itertools::Itertools;
use std::sync::Arc;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::{Result, ResultWithContext};
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::Impute;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Impute {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        // Create ScalarValue used to fill in null values
        let json_value: serde_json::Value = serde_json::from_str(
            &self
                .value_json
                .clone()
                .unwrap_or_else(|| "null".to_string()),
        )?;

        // JSON numbers are always interpreted as floats, but if the value is an integer we'd
        // like the fill value to be an integer as well to avoid converting an integer input
        // column to floats
        let value = if json_value.is_null() {
            ScalarValue::Float64(None)
        } else if json_value.is_i64() {
            ScalarValue::from(json_value.as_i64().unwrap())
        } else if json_value.is_f64() && json_value.as_f64().unwrap().fract() == 0.0 {
            ScalarValue::from(json_value.as_f64().unwrap() as i64)
        } else {
            ScalarValue::from_json(&json_value)?
        };

        // Take unique groupby fields
        let groupby = self
            .groupby
            .clone()
            .into_iter()
            .unique()
            .collect::<Vec<_>>();

        // Unescape field, key, and groupby fields
        let field = unescape_field(&self.field);
        let key = unescape_field(&self.key);
        let groupby: Vec<_> = groupby.iter().map(|f| unescape_field(f)).collect();

        let dataframe = dataframe
            .impute(&field, value, &key, groupby.as_slice(), Some(ORDER_COL))
            .await
            .with_context(|| "Impute transform failed".to_string())?;

        Ok((dataframe, Vec::new()))
    }
}
