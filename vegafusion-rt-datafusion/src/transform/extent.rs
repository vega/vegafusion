use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::to_numeric;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::utils::RecordBatchUtils;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Field;

use datafusion::common::DFSchema;

use crate::expression::escape::unescaped_col;
use datafusion::logical_expr::{max, min};
use datafusion::scalar::ScalarValue;
use datafusion_expr::Expr;
use std::sync::Arc;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Extent;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Extent {
    async fn eval(
        &self,
        sql_df: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let output_values = if self.signal.is_some() {
            let (min_expr, max_expr) = min_max_exprs(self.field.as_str(), &sql_df.schema_df())?;

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
    let min_val_array = result_rb.column_by_name("__min_val")?;
    let max_val_array = result_rb.column_by_name("__max_val")?;

    let min_val_scalar = ScalarValue::try_from_array(min_val_array, 0).unwrap();
    let max_val_scalar = ScalarValue::try_from_array(max_val_array, 0).unwrap();

    // Build two-element list of the extents
    let element_datatype = min_val_scalar.get_datatype();
    let extent_list = TaskValue::Scalar(ScalarValue::List(
        Some(vec![min_val_scalar, max_val_scalar]),
        Box::new(Field::new("item", element_datatype, true)),
    ));
    Ok(extent_list)
}
