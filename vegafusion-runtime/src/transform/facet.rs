use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, lit, max};
use std::sync::Arc;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_common::escape::unescape_field;
use vegafusion_core::arrow::datatypes::Schema;
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
                let group_field = unescape_field(&self.groupby[0]);
                let group_col = unescaped_col(&self.groupby[0]);

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
                    for tx in &self.transform {
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
                            if f.name() == &group_field {
                                None
                            } else {
                                Some(flat_col(f.name()))
                            }
                        })
                        .collect::<Vec<_>>();
                    selections.insert(0, lit(col_value).alias(&group_field));
                    filtered_df = filtered_df.select(selections).await?;

                    // Grab schema
                    final_schema = filtered_df.schema();

                    // evaluate to batches
                    let final_table = filtered_df.collect().await?;
                    batches.extend(final_table.batches);
                }

                let combined = VegaFusionTable::try_new(Arc::new(final_schema), batches)?;
                let df = dataframe.connection().scan_arrow(combined).await?;
                (df, output_values)
            }
            2 => {
                todo!("Facetting by two columns not yet implemented")
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
