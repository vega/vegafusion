use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use datafusion_expr::lit;
use std::sync::Arc;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_common::escape::unescape_field;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::proto::gen::transforms::Transform;
use vegafusion_core::{proto::gen::transforms::Facet, task_graph::task_value::TaskValue};
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Facet {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let (df, output_values) = match self.groupby.len() {
            1 => {
                facet_one_column(
                    dataframe,
                    config,
                    &self.groupby[0],
                    self.transform.as_slice(),
                )
                .await?
            }
            2 => {
                facet_two_columns(
                    dataframe,
                    config,
                    &self.groupby[0],
                    &self.groupby[1],
                    self.transform.as_slice(),
                )
                .await?
            }
            i => {
                return Err(VegaFusionError::internal(format!(
                    "Unexpected groupby length for facet transform: {i}"
                )))
            }
        };

        Ok((df, output_values))
    }
}

async fn facet_one_column(
    dataframe: Arc<dyn DataFrame>,
    config: &CompilationConfig,
    group_field: &String,
    transforms: &[Transform],
) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
    // Unescape field
    let unescaped_group_field = unescape_field(&group_field);
    let group_col = unescaped_col(&group_field);

    // Collect unique values of group column
    let table = dataframe
        .aggregate(vec![group_col.clone()], vec![])
        .await?
        .collect()
        .await?;
    let batch = table.to_record_batch()?;
    let col_array = batch.column(0);
    let col_values = (0..col_array.len())
        .map(|i| Ok(ScalarValue::try_from_array(col_array, i)?))
        .collect::<Result<Vec<_>>>()?;

    // Collect output schema and batches
    let mut final_schema = Schema::empty();
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut output_values: Vec<TaskValue> = Vec::new();
    for col_value in col_values {
        let filter = if col_value.is_null() {
            group_col.clone().is_null()
        } else {
            group_col.clone().eq(lit(col_value.clone()))
        };

        let mut filtered_df = dataframe.filter(filter).await?;

        // Apply transform pipeline
        for tx in transforms {
            let (tx_df, tx_out_vals) = tx.eval(filtered_df, config).await?;
            output_values.extend(tx_out_vals);
            filtered_df = tx_df;
        }

        // Add group value column back
        let mut selections = filtered_df
            .schema()
            .fields
            .iter()
            .filter_map(|f| {
                if f.name() == &unescaped_group_field {
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            })
            .collect::<Vec<_>>();
        selections.insert(0, lit(col_value).alias(&unescaped_group_field));
        filtered_df = filtered_df.select(selections).await?;

        // Grab schema
        final_schema = filtered_df.schema();

        // evaluate to batches
        let final_table = filtered_df.collect().await?;
        batches.extend(final_table.batches);
    }

    let combined = VegaFusionTable::try_new(Arc::new(final_schema), batches)?;
    let df = dataframe.connection().scan_arrow(combined).await?;
    Ok((df, output_values))
}

async fn facet_two_columns(
    dataframe: Arc<dyn DataFrame>,
    config: &CompilationConfig,
    group_field0: &String,
    group_field1: &String,
    transforms: &[Transform],
) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
    // Unescape fields
    let unescaped_group_field0 = unescape_field(&group_field0);
    let group_col0 = unescaped_col(&group_field0);

    let unescaped_group_field1 = unescape_field(&group_field1);
    let group_col1 = unescaped_col(&group_field1);

    // Collect unique values of group column
    let table = dataframe
        .aggregate(vec![group_col0.clone(), group_col1.clone()], vec![])
        .await?
        .collect()
        .await?;
    let batch = table.to_record_batch()?;
    let col_array0 = batch.column(0);
    let col_array1 = batch.column(1);

    let col_values = (0..col_array0.len())
        .map(|i| {
            let scalar0 = ScalarValue::try_from_array(col_array0, i)?;
            let scalar1 = ScalarValue::try_from_array(col_array1, i)?;
            Ok((scalar0, scalar1))
        })
        .collect::<Result<Vec<_>>>()?;

    // Collect output schema and batches
    let mut final_schema = Schema::empty();
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut output_values: Vec<TaskValue> = Vec::new();
    for (col0_value, col1_value) in col_values {
        let filter0 = if col0_value.is_null() {
            group_col0.clone().is_null()
        } else {
            group_col0.clone().eq(lit(col0_value.clone()))
        };
        let filter1 = if col1_value.is_null() {
            group_col1.clone().is_null()
        } else {
            group_col1.clone().eq(lit(col1_value.clone()))
        };

        let mut filtered_df = dataframe.filter(filter0.and(filter1)).await?;

        // Apply transform pipeline
        for tx in transforms {
            let (tx_df, tx_out_vals) = tx.eval(filtered_df, config).await?;
            output_values.extend(tx_out_vals);
            filtered_df = tx_df;
        }

        // Add group value column back
        let mut selections = filtered_df
            .schema()
            .fields
            .iter()
            .filter_map(|f| {
                if f.name() == &unescaped_group_field0 || f.name() == &unescaped_group_field1 {
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            })
            .collect::<Vec<_>>();
        selections.insert(0, lit(col1_value).alias(&unescaped_group_field1));
        selections.insert(0, lit(col0_value).alias(&unescaped_group_field0));
        filtered_df = filtered_df.select(selections).await?;

        // Grab schema
        final_schema = filtered_df.schema();

        // evaluate to batches
        let final_table = filtered_df.collect().await?;
        batches.extend(final_table.batches);
    }

    // Build combined DataFrame
    let combined = VegaFusionTable::try_new(Arc::new(final_schema), batches)?;
    let df = dataframe.connection().scan_arrow(combined).await?;
    Ok((df, output_values))
}
