use crate::arrow::{
    compute::concat_batches,
    datatypes::{DataType, SchemaRef},
    json,
    record_batch::RecordBatch,
};
use crate::error::{Result, ResultWithContext, VegaFusionError};
use std::borrow::Cow;

use std::convert::TryFrom;
use std::sync::Arc;

use crate::arrow::ipc::reader::StreamReader;
use crate::arrow::ipc::writer::StreamWriter;
use std::hash::{Hash, Hasher};
use std::io::Cursor;

use super::scalar::ScalarValue;
use crate::arrow::array::ArrayRef;
use crate::data::json_writer::record_batches_to_json_rows;

use crate::data::{ORDER_COL, ORDER_COL_DTYPE};
use arrow::array::{StructArray, UInt32Array};
use arrow::datatypes::{Field, Schema};
use arrow::json::reader::DecoderOptions;
use serde_json::{json, Value};

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

    pub fn empty_with_ordering() -> Self {
        // Return empty table with single ORDER_COL column
        let empty_record_batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            ORDER_COL,
            DataType::UInt64,
            false,
        )])));
        VegaFusionTable::from(empty_record_batch)
    }

    pub fn with_ordering(self) -> Result<Self> {
        // Build new schema with leading ORDER_COL
        let mut new_fields = self.schema.fields.clone();
        let mut start_idx = 0;
        if new_fields.is_empty() {
            return Ok(Self::empty_with_ordering());
        }
        let leading_field = new_fields
            .get(0)
            .expect("VegaFusionTable must have at least one column");
        let has_order_col = if leading_field.name() == ORDER_COL {
            // There is already a leading ORDER_COL, remove it and replace below
            new_fields.remove(0);
            true
        } else {
            // We need to add a new leading field for the ORDER_COL
            false
        };
        new_fields.insert(0, Field::new(ORDER_COL, ORDER_COL_DTYPE, false));

        let new_schema = Arc::new(Schema::new(new_fields)) as SchemaRef;

        let new_batches = self
            .batches
            .into_iter()
            .map(|batch| {
                let order_array = Arc::new(UInt32Array::from_iter_values(
                    start_idx..(start_idx + batch.num_rows() as u32),
                )) as ArrayRef;

                let mut new_columns = Vec::from(batch.columns());

                if has_order_col {
                    new_columns[0] = order_array;
                } else {
                    new_columns.insert(0, order_array);
                }

                start_idx += batch.num_rows() as u32;

                Ok(RecordBatch::try_new(new_schema.clone(), new_columns)?)
            })
            .collect::<Result<Vec<_>>>()?;

        Self::try_new(new_schema, new_batches)
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut schema = self.schema.clone();
        if let Some(batch) = self.batches.get(0) {
            schema = batch.schema()
        }
        concat_batches(&schema, &self.batches)
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        if self.num_rows() == 0 {
            // Return empty list with (arbitrary) Float64 type
            let dtype = DataType::Float64;
            return Ok(ScalarValue::List(
                Some(Vec::new()),
                Box::new(Field::new("item", dtype, true)),
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
        Ok(ScalarValue::List(
            Some(elements),
            Box::new(Field::new("item", dtype, true)),
        ))
    }

    pub fn to_json(&self) -> Result<serde_json::Value> {
        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(self.num_rows());
        for row in record_batches_to_json_rows(&self.batches)? {
            rows.push(serde_json::Value::Object(row));
        }
        Ok(serde_json::Value::Array(rows))
    }

    pub fn from_json(value: &serde_json::Value, batch_size: usize) -> Result<Self> {
        if let serde_json::Value::Array(values) = value {
            // Handle special case where array elements are non-object scalars
            let mut values = Cow::Borrowed(values);
            if let Some(first) = values.get(0) {
                if let Value::Object(props) = first {
                    // Handle odd special case where vega will interpret
                    // [{}, {}] as [{"datum": {}}, {"datum": {}}]
                    if props.is_empty() {
                        values = Cow::Owned(vec![json!({"datum": {"__dummy": 0}}); values.len()])
                    }
                } else {
                    // Array of scalars, need to wrap elements objects with "data" field
                    values = Cow::Owned(
                        values
                            .iter()
                            .map(|value| json!({ "data": value }))
                            .collect(),
                    )
                }
            }

            let schema_result = json::reader::infer_json_schema_from_iterator(
                values.iter().take(1024).map(|v| Ok(v.clone())),
            );

            match schema_result {
                Err(_) => {
                    // This happens when array elements are objects with no fields
                    let empty_scalar = ScalarValue::from(vec![(
                        "__dummy",
                        ScalarValue::try_from(&DataType::Float64).unwrap(),
                    )]);
                    let array = empty_scalar.to_array_of_size(values.len());
                    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                    let record_batch = RecordBatch::from(struct_array);
                    Self::try_new(record_batch.schema(), vec![record_batch])
                }
                Ok(schema) => {
                    let schema_ref = Arc::new(schema);

                    // read record batches
                    let decoder = json::reader::Decoder::new(
                        schema_ref.clone(),
                        DecoderOptions::default().with_batch_size(batch_size),
                    );
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
            }
        } else {
            Err(VegaFusionError::internal(format!(
                "Expected JSON array, not: {}",
                value
            )))
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
        let reader = StreamReader::try_new(cursor, None)?;
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

#[cfg(test)]
mod tests {
    use crate::data::table::VegaFusionTable;
    use serde_json::json;

    #[test]
    fn test_with_ordering() {
        let table1 = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": "A"},
                {"a": 2, "b": "BB"},
                {"a": 10, "b": "CCC"},
                {"a": 20, "b": "DDDD"},
            ]),
            2,
        )
        .unwrap();
        assert_eq!(table1.batches.len(), 2);

        let table2 = VegaFusionTable::from_json(
            &json!([
                {"_vf_order": 10u32, "a": 1, "b": "A"},
                {"_vf_order": 9u32, "a": 2, "b": "BB"},
                {"_vf_order": 8u32, "a": 10, "b": "CCC"},
                {"_vf_order": 7u32, "a": 20, "b": "DDDD"},
            ]),
            2,
        )
        .unwrap();
        assert_eq!(table2.batches.len(), 2);

        let expected_json = json!([
            {"_vf_order": 0u32, "a": 1, "b": "A"},
            {"_vf_order": 1u32, "a": 2, "b": "BB"},
            {"_vf_order": 2u32, "a": 10, "b": "CCC"},
            {"_vf_order": 3u32, "a": 20, "b": "DDDD"},
        ]);

        // Add ordering column to table without one
        let result_table1 = table1.with_ordering().unwrap();
        assert_eq!(result_table1.batches.len(), 2);
        assert_eq!(result_table1.to_json().unwrap(), expected_json);

        // Override prior ordering column
        let result_table2 = table2.with_ordering().unwrap();
        assert_eq!(result_table2.batches.len(), 2);
        assert_eq!(result_table2.to_json().unwrap(), expected_json);
    }
}
