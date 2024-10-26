use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use crate::data::util::DataFrameUtils;
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::DataFrame;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::Expr;
use datafusion_functions_aggregate::expr_fn::{max, min};
use std::sync::Arc;
use vegafusion_common::column::unescaped_col;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_common::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Extent;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Extent {
    async fn eval(
        &self,
        sql_df: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let output_values = if self.signal.is_some() {
            let (min_expr, max_expr) = min_max_exprs(self.field.as_str(), sql_df.schema())?;

            let extent_df = sql_df
                .clone()
                .aggregate(Vec::new(), vec![min_expr, max_expr])?;

            // Eval to single row dataframe and extract scalar values
            let result_batch = extent_df.collect_flat().await?;
            let extent_list = extract_extent_list(&result_batch)?;
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

fn extract_extent_list(batch: &RecordBatch) -> Result<TaskValue> {
    let min_val_array = batch
        .column_by_name("__min_val")
        .with_context(|| "No column named __min_val".to_string())?;
    let max_val_array = batch
        .column_by_name("__max_val")
        .with_context(|| "No column named __max_val".to_string())?;

    let min_val_scalar = ScalarValue::try_from_array(min_val_array, 0).unwrap();
    let max_val_scalar = ScalarValue::try_from_array(max_val_array, 0).unwrap();

    // Build two-element list of the extents
    let extent_list = TaskValue::Scalar(ScalarValue::List(Arc::new(array_into_list_array(
        ScalarValue::iter_to_array(vec![min_val_scalar, max_val_scalar])?,
        true,
    ))));
    Ok(extent_list)
}
