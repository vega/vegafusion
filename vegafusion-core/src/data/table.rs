use std::sync::Arc;
use crate::error::{Result, VegaFusionError, ResultWithContext};
use crate::arrow::{
    json,
    record_batch::RecordBatch,
    datatypes::{SchemaRef, Schema, Field, DataType},
};

use crate::proto::gen::expression::literal::Value;
use crate::arrow::json::writer::record_batches_to_json_rows;
use crate::arrow::ipc::writer::StreamWriter;
use std::io::Cursor;
use crate::arrow::ipc::reader::StreamReader;
use std::hash::{Hash, Hasher};
use crate::proto::gen::tasks::TaskValue;
use crate::proto::gen::tasks::task_value;
use std::convert::TryFrom;
use super::scalar::ScalarValue;
use arrow::array::StructArray;
use crate::arrow::array::ArrayRef;


#[derive(Clone, Debug)]
pub struct VegaFusionTable {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl VegaFusionTable {
    pub fn try_new(schema: SchemaRef, partitions: Vec<RecordBatch>) -> Result<Self> {
        if partitions
            .iter()
            .all(|batches| schema.contains(&batches.schema()))
        {
            Ok(Self {
                schema,
                batches: partitions,
            })
        } else {
            Err(VegaFusionError::internal(
                "Mismatch between schema and batches",
            ))
        }
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Keep, at most, the first n rows
    pub fn head(&self, n: usize) -> Self {
        let mut so_far = 0;
        let mut head_batches: Vec<RecordBatch> = Vec::new();

        for batch in &self.batches {
            if so_far == n {
                break;
            } else if so_far + batch.num_rows() <= n {
                // Keep full batch
                so_far += batch.num_rows();
                head_batches.push(batch.clone());
            } else {
                // Keep partial batch
                let keep = n - so_far;
                head_batches.push(batch.slice(0, keep));
                break;
            }
        }

        Self {
            schema: self.schema.clone(),
            batches: head_batches,
        }
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        RecordBatch::concat(&self.schema, &self.batches)
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        if self.num_rows() == 0 {
            // Return empty list with (arbitrary) Float64 type
            let dtype = DataType::Float64;
            return Ok(ScalarValue::List(
                Some(Box::new(Vec::new())),
                Box::new(dtype),
            ));
        }

        let mut elements: Vec<ScalarValue> = Vec::new();
        for batch in &self.batches {
            let array = Arc::new(StructArray::from(batch.clone())) as ArrayRef;

            for i in 0..array.len() {
                let scalar = ScalarValue::try_from_array(&array, i).with_context(|| {
                    "Failed to convert record batch row to ScalarValue".to_string()
                })?;
                elements.push(scalar)
            }
        }

        let dtype = elements[0].get_datatype();
        Ok(ScalarValue::List(Some(Box::new(elements)), Box::new(dtype)))
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(self.num_rows());
        for row in record_batches_to_json_rows(&self.batches) {
            rows.push(serde_json::Value::Object(row));
        }
        serde_json::Value::Array(rows)
    }

    pub fn from_json(value: serde_json::Value, batch_size: usize) -> Result<Self> {
        if let serde_json::Value::Array(values) = &value {
            if values.is_empty() {
                // Create empty record batch
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "__dummy",
                    DataType::Float64,
                    true,
                )]));
                Self::try_new(schema.clone(), vec![RecordBatch::new_empty(schema)])
            } else {
                // Infer schema
                let schema = json::reader::infer_json_schema_from_iterator(
                    values.iter().take(1024).map(|v| Ok(v.clone())),
                )
                    .with_context(|| "Failed to infer schema")?;
                let schema_ref = Arc::new(schema);

                // read record batches
                let decoder = json::reader::Decoder::new(schema_ref.clone(), batch_size, None);
                let mut batches: Vec<RecordBatch> = Vec::new();
                let mut value_iter = values.iter().map(|v| Ok(v.clone()));

                while let Some(batch) = decoder
                    .next_batch(&mut value_iter)
                    .with_context(|| "Failed to read json to arrow")?
                {
                    batches.push(batch);
                }

                Self::try_new(schema_ref, batches)
            }
        } else {
            return Err(VegaFusionError::internal(&format!(
                "Expected JSON array, not: {}",
                value
            )));
        }
    }

    // Serialize to bytes using Arrow IPC format
    pub fn to_ipc_bytes(&self) -> Result<Vec<u8>> {
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer = StreamWriter::try_new(buffer, self.schema.as_ref())?;

        for batch in &self.batches {
            stream_writer.write(batch)?;
        }

        stream_writer.finish()?;
        Ok(stream_writer.into_inner()?)
    }

    pub fn from_ipc_bytes(data: &[u8]) -> Result<Self> {
        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor)?;
        let schema = reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();

        for batch in reader {
            batches.push(batch?);
        }

        Ok(Self { schema, batches })
    }
}

impl From<RecordBatch> for VegaFusionTable {
    fn from(value: RecordBatch) -> Self {
        Self {
            schema: value.schema(),
            batches: vec![value],
        }
    }
}

impl Hash for VegaFusionTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_ipc_bytes().unwrap().hash(state)
    }
}
