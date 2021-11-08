use crate::expression::compiler::config::CompilationConfig;
use crate::transform::utils::{DataFrameUtils, RecordBatchUtils};
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{col, max, min};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Extent;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Extent {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let output_values = if self.signal.is_some() {
            let field_col = col(self.field.as_str());
            let min_val = min(field_col.clone()).alias("__min_val");
            let max_val = max(field_col).alias("__max_val");

            let extent_df = dataframe
                .aggregate(Vec::new(), vec![min_val, max_val])
                .unwrap();

            // Eval to single row dataframe and extract scalar values
            let result_rb = extent_df.collect_flat().await?;
            let min_val_array = result_rb.column_by_name("__min_val")?;
            let max_val_array = result_rb.column_by_name("__max_val")?;

            let min_val_scalar = ScalarValue::try_from_array(min_val_array, 0).unwrap();
            let max_val_scalar = ScalarValue::try_from_array(max_val_array, 0).unwrap();

            // Build two-element list of the extents
            let element_datatype = min_val_scalar.get_datatype();
            let extent_list = TaskValue::Scalar(ScalarValue::List(
                Some(Box::new(vec![min_val_scalar, max_val_scalar])),
                Box::new(element_datatype),
            ));
            vec![extent_list]
        } else {
            Vec::new()
        };

        Ok((dataframe, output_values))
    }
}
