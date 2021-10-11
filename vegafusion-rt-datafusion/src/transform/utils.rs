use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use crate::tokio_runtime::TOKIO_RUNTIME;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use std::sync::Arc;

pub trait DataFrameUtils {
    fn block_eval(&self) -> Result<Vec<RecordBatch>>;
    fn block_flat_eval(&self) -> Result<RecordBatch>;
}

impl DataFrameUtils for Arc<dyn DataFrame> {
    fn block_eval(&self) -> Result<Vec<RecordBatch>> {
        // Not partitioned (this is faster sometimes?)
        let res = TOKIO_RUNTIME
            .block_on(self.collect())
            .with_context(|| "Failed to collect DataFrame".to_string())?;
        Ok(res)
    }

    fn block_flat_eval(&self) -> Result<RecordBatch> {
        let arrow_schema = Arc::new(self.schema().into()) as SchemaRef;
        let batches = self.block_eval()?;
        RecordBatch::concat(&arrow_schema, &batches)
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }
}

pub trait RecordBatchUtils {
    fn column_by_name(&self, name: &str) -> Result<&ArrayRef>;
    fn equals(&self, other: &RecordBatch) -> bool;
}

impl RecordBatchUtils for RecordBatch {
    fn column_by_name(&self, name: &str) -> Result<&ArrayRef> {
        match self.schema().column_with_name(name) {
            Some((index, _)) => Ok(self.column(index)),
            None => Err(VegaFusionError::internal(&format!(
                "No column named {}",
                name
            ))),
        }
    }

    fn equals(&self, other: &RecordBatch) -> bool {
        if self.schema() != other.schema() {
            // Schema's are not equal
            return false;
        }

        // Schema's equal, check columns
        let schema = self.schema();

        for (i, _field) in schema.fields().iter().enumerate() {
            let self_array = self.column(i);
            let other_array = other.column(i);
            if self_array != other_array {
                return false;
            }
        }

        true
    }
}
