use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use crate::transform::utils::DataFrameUtils;
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::json::writer::record_batches_to_json_rows;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::{datatypes::SchemaRef, json, record_batch::RecordBatch};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    from = "SerializableVegaFusionTable",
    into = "SerializableVegaFusionTable"
)]
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

    pub fn to_memtable(&self) -> MemTable {
        // Unwrap is safe because we perform the MemTable validation in our try_new function
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()]).unwrap()
    }

    pub fn to_dataframe(&self) -> Result<Arc<dyn DataFrame>> {
        let mut ctx = ExecutionContext::new();
        let provider = self.to_memtable();
        ctx.register_table("df", Arc::new(provider)).unwrap();
        ctx.table("df")
            .with_context(|| "Failed to create DataFrame".to_string())
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

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        RecordBatch::concat(&self.schema, &self.batches)
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    pub fn pretty_format(&self, max_rows: Option<usize>) -> Result<String> {
        if let Some(max_rows) = max_rows {
            pretty_format_batches(&self.head(max_rows).batches)
                .with_context(|| String::from("Failed to pretty print"))
        } else {
            pretty_format_batches(&self.batches)
                .with_context(|| String::from("Failed to pretty print"))
        }
    }

    pub fn to_json(&self) -> Value {
        let mut rows: Vec<Value> = Vec::with_capacity(self.num_rows());
        for row in record_batches_to_json_rows(&self.batches) {
            rows.push(Value::Object(row));
        }
        Value::Array(rows)
    }

    pub fn from_json(value: Value, batch_size: usize) -> Result<Self> {
        if let Value::Array(values) = &value {
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
}

impl From<RecordBatch> for VegaFusionTable {
    fn from(value: RecordBatch) -> Self {
        Self {
            schema: value.schema(),
            batches: vec![value],
        }
    }
}

impl TryFrom<Arc<dyn DataFrame>> for VegaFusionTable {
    type Error = VegaFusionError;

    fn try_from(value: Arc<dyn DataFrame>) -> std::result::Result<Self, Self::Error> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.block_eval()?;
        Ok(Self { schema, batches })
    }
}

impl Hash for VegaFusionTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        SerializableVegaFusionTable::from(self).hash(state);
    }
}

// Serialization
#[derive(Clone, Debug, Serialize, Deserialize, Hash)]
pub struct SerializableVegaFusionTable {
    batches: Vec<u8>,
}

impl From<&VegaFusionTable> for SerializableVegaFusionTable {
    fn from(value: &VegaFusionTable) -> Self {
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer = StreamWriter::try_new(buffer, value.schema.as_ref()).unwrap();

        for batch in &value.batches {
            stream_writer.write(batch).unwrap();
        }

        stream_writer.finish().unwrap();
        Self {
            batches: stream_writer.into_inner().unwrap(),
        }
    }
}

impl From<VegaFusionTable> for SerializableVegaFusionTable {
    fn from(value: VegaFusionTable) -> Self {
        Self::from(&value)
    }
}

impl From<SerializableVegaFusionTable> for VegaFusionTable {
    fn from(value: SerializableVegaFusionTable) -> Self {
        let cursor = Cursor::new(value.batches);
        let reader = StreamReader::try_new(cursor).unwrap();
        let schema = reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();

        for batch in reader {
            batches.push(batch.expect("Failed to deserialize record batch"));
        }

        Self { schema, batches }
    }
}
