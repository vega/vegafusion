use crate::data::util::DataFrameUtils;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::aggregate::make_agg_expr_for_col_expr;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_expr::{lit, when};
use datafusion_functions_aggregate::expr_fn::min;
use vegafusion_common::arrow::array::StringArray;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::scalar::ScalarValue;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::{cast_to, data_type, is_string_datatype, to_numeric};
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::{AggregateOp, Pivot};
use vegafusion_core::task_graph::task_value::TaskValue;

/// NULL_PLACEHOLDER_NAME is used for sorting to match Vega, where null always comes first for
/// limit sorting
const NULL_PLACEHOLDER_NAME: &str = "!!!null";

/// NULL_NAME is the final column name for null columns
const NULL_NAME: &str = "null";

#[async_trait]
impl TransformTrait for Pivot {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        // Make sure the pivot column is a string
        let pivot_dtype = data_type(&unescaped_col(&self.field), dataframe.schema())?;
        let dataframe = if matches!(pivot_dtype, DataType::Boolean) {
            // Boolean column type. For consistency with vega, replace 0 with "false" and 1 with "true"
            let select_exprs: Vec<_> = dataframe
                .schema()
                .inner()
                .fields
                .iter()
                .map(|field| {
                    if field.name() == &unescape_field(&self.field) {
                        Ok(when(unescaped_col(&self.field).eq(lit(true)), lit("true"))
                            .when(
                                unescaped_col(&self.field).is_null(),
                                lit(NULL_PLACEHOLDER_NAME),
                            )
                            .otherwise(lit("false"))
                            .with_context(|| "Failed to construct Case expression")?
                            .alias(&self.field))
                    } else {
                        Ok(flat_col(field.name()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            dataframe.select(select_exprs)?
        } else if !is_string_datatype(&pivot_dtype) {
            // Column type is not string, so cast values to strings
            let select_exprs: Vec<_> = dataframe
                .schema()
                .inner()
                .fields
                .iter()
                .map(|field| {
                    if field.name() == &unescape_field(&self.field) {
                        Ok(when(
                            unescaped_col(&self.field).is_null(),
                            lit(NULL_PLACEHOLDER_NAME),
                        )
                        .otherwise(cast_to(
                            unescaped_col(&self.field),
                            &DataType::Utf8,
                            dataframe.schema(),
                        )?)?
                        .alias(&self.field))
                    } else {
                        Ok(flat_col(field.name()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            dataframe.select(select_exprs)?
        } else {
            // Column type is string, just replace NULL with "null"
            let select_exprs: Vec<_> = dataframe
                .schema()
                .inner()
                .fields
                .iter()
                .map(|field| {
                    if field.name() == &unescape_field(&self.field) {
                        let field_col = unescaped_col(&self.field);
                        Ok(
                            when(field_col.clone().is_null(), lit(NULL_PLACEHOLDER_NAME))
                                .when(field_col.clone().eq(lit("")), lit(" "))
                                .otherwise(field_col)?
                                .alias(&self.field),
                        )
                    } else {
                        Ok(flat_col(field.name()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            dataframe.select(select_exprs)?
        };

        pivot_case(self, dataframe).await
    }
}

async fn extract_sorted_pivot_values(tx: &Pivot, dataframe: DataFrame) -> Result<Vec<String>> {
    let agg_query = dataframe.aggregate_mixed(vec![unescaped_col(&tx.field)], vec![])?;

    let limit = match tx.limit {
        None | Some(0) => None,
        Some(i) => Some(i as usize),
    };

    let sorted_query = agg_query
        .sort(vec![unescaped_col(&tx.field).sort(true, false)])?
        .limit(0, limit)?;

    let pivot_batch = sorted_query.collect_flat().await?;
    let pivot_array = pivot_batch
        .column_by_name(&tx.field)
        .with_context(|| format!("No column named {}", tx.field))?;
    let pivot_array = pivot_array
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| "Failed to downcast pivot column to String")?;
    let pivot_vec: Vec<_> = pivot_array
        .iter()
        .filter_map(|val| val.map(|s| s.to_string()))
        .collect();
    Ok(pivot_vec)
}

async fn pivot_case(tx: &Pivot, dataframe: DataFrame) -> Result<(DataFrame, Vec<TaskValue>)> {
    let pivot_vec = extract_sorted_pivot_values(tx, dataframe.clone()).await?;

    if pivot_vec.is_empty() {
        return Err(VegaFusionError::internal("Unexpected empty pivot dataset"));
    }

    let schema = dataframe.schema();

    // Process aggregate operation
    let agg_op: AggregateOp = tx
        .op
        .map(|op_code| AggregateOp::try_from(op_code).unwrap())
        .unwrap_or(AggregateOp::Sum);

    // Build vector of aggregates
    let mut agg_exprs: Vec<_> = Vec::new();

    for pivot_val in pivot_vec.iter() {
        let predicate_expr = unescaped_col(&tx.field).eq(lit(pivot_val.as_str()));
        let value_expr = to_numeric(unescaped_col(tx.value.as_str()), schema)?;
        let agg_col = when(predicate_expr, value_expr).otherwise(lit(ScalarValue::Null))?;

        let agg_expr = make_agg_expr_for_col_expr(agg_col, &agg_op, schema)?;

        // Compute pivot column name, replacing null placeholder with "null"
        let col_name = if pivot_val == NULL_PLACEHOLDER_NAME {
            NULL_NAME
        } else {
            pivot_val.as_str()
        };
        let agg_expr = agg_expr.alias(col_name);

        agg_exprs.push(agg_expr);
    }

    // Insert ordering aggregate
    agg_exprs.insert(0, min(flat_col(ORDER_COL)).alias(ORDER_COL));

    // Build vector of groupby expressions
    let group_expr: Vec<_> = tx.groupby.iter().map(|c| unescaped_col(c)).collect();

    let pivoted = dataframe.aggregate_mixed(group_expr, agg_exprs)?;
    Ok((pivoted, Default::default()))
}
