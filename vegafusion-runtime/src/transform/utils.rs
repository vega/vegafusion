use vegafusion_common::arrow::array::ArrayRef;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait RecordBatchUtils {
    fn column_by_name(&self, name: &str) -> Result<&ArrayRef>;
    fn equals(&self, other: &RecordBatch) -> bool;
}

impl RecordBatchUtils for RecordBatch {
    fn column_by_name(&self, name: &str) -> Result<&ArrayRef> {
        match self.schema().column_with_name(name) {
            Some((index, _)) => Ok(self.column(index)),
            None => Err(VegaFusionError::internal(format!("No column named {name}"))),
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
