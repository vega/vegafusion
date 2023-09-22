use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{max, min, Expr};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::Field;
use vegafusion_common::column::unescaped_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Extent;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Extent {
    async fn eval(
        &self,
        sql_df: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let output_values = if self.signal.is_some() {
            let (min_expr, max_expr) = min_max_exprs(self.field.as_str(), &sql_df.schema_df()?)?;

            let extent_df = sql_df
                .aggregate(Vec::new(), vec![min_expr, max_expr])
                .await
                .unwrap();

            // Eval to single row dataframe and extract scalar values
            let result_table = extent_df.collect().await?;
            let extent_list = extract_extent_list(&result_table)?;
            vec![extent_list]
        } else {
            Vec::new()
        };

        Ok((sql_df, output_values))
    }
}

fn min_max_exprs(field: &str, schema: &DFSchema) -> Result<(Expr, Expr)> {
    let field_col = unescaped_col(field);
    let min_expr = min(to_numeric(field_col.clone(), schema)?).alias("__min_val");
    let max_expr = max(to_numeric(field_col, schema)?).alias("__max_val");
    Ok((min_expr, max_expr))
}

fn extract_extent_list(table: &VegaFusionTable) -> Result<TaskValue> {
    let result_rb = table.to_record_batch()?;
    let min_val_array = result_rb
        .column_by_name("__min_val")
        .with_context(|| "No column named __min_val".to_string())?;
    let max_val_array = result_rb
        .column_by_name("__max_val")
        .with_context(|| "No column named __max_val".to_string())?;

    let min_val_scalar = ScalarValue::try_from_array(min_val_array, 0).unwrap();
    let max_val_scalar = ScalarValue::try_from_array(max_val_array, 0).unwrap();

    // Build two-element list of the extents
    let element_datatype = min_val_scalar.data_type();
    let extent_list = TaskValue::Scalar(ScalarValue::List(
        Some(vec![min_val_scalar, max_val_scalar]),
        Arc::new(Field::new("item", element_datatype, true)),
    ));
    Ok(extent_list)
}
