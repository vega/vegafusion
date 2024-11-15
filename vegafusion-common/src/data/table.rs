use datafusion_common::ScalarValue;

use ahash::RandomState;
use arrow::{
    array::{ArrayData, ArrayRef, StructArray, UInt32Array},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};

use crate::{
    data::{ORDER_COL, ORDER_COL_DTYPE},
    error::{Result, ResultWithContext, VegaFusionError},
};

use arrow::array::{
    new_empty_array, Array, ArrowPrimitiveType, AsArray, BinaryViewArray, GenericBinaryArray,
    GenericStringArray, NullArray, OffsetSizeTrait, PrimitiveArray, StringViewArray,
};
#[cfg(feature = "prettyprint")]
use arrow::util::pretty::pretty_format_batches;
use std::{
    hash::{Hash, Hasher},
    io::Cursor,
    sync::Arc,
};

use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, ToByteSlice,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
#[cfg(feature = "json")]
use {
    crate::data::json_writer::record_batches_to_json_rows,
    arrow::json,
    serde_json::{json, Value},
    std::{borrow::Cow, convert::TryFrom},
};

#[cfg(feature = "py")]
use {
    pyo3::{
        prelude::*,
        types::{PyDict, PyList},
        Bound, PyAny, PyErr, Python,
    },
    pyo3_arrow::{PyRecordBatch, PySchema, PyTable},
};

#[cfg(feature = "base64")]
use base64::{engine::general_purpose, Engine as _};
use datafusion_common::utils::array_into_list_array;

#[derive(Clone, Debug)]
pub struct VegaFusionTable {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl VegaFusionTable {
    pub fn try_new(schema: SchemaRef, partitions: Vec<RecordBatch>) -> Result<Self> {
        // Make all columns nullable
        let schema_fields: Vec<_> = schema
            .fields
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        if partitions.iter().all(|batch| {
            let batch_schema_fields: Vec<_> = batch
                .schema()
                .fields
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect();
            let batch_schema = Arc::new(Schema::new(batch_schema_fields));
            schema.fields.contains(&batch_schema.fields)
        }) {
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
            true,
        )])));
        VegaFusionTable::from(empty_record_batch)
    }

    pub fn without_ordering(self) -> Result<Self> {
        let mut new_fields = self.schema.fields.to_vec();
        if new_fields.len() == 1 {
            // Only one column, so we can't remove it, even if it's an ordering column
            return Ok(self);
        }
        let order_col_index = self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_i, field)| field.name() == ORDER_COL)
            .map(|(i, _)| i);

        if let Some(order_col_index) = order_col_index {
            new_fields.remove(order_col_index);

            let new_schema = Arc::new(Schema::new(new_fields)) as SchemaRef;
            let new_batches = self
                .batches
                .into_iter()
                .map(|batch| {
                    let mut new_columns = Vec::from(batch.columns());
                    new_columns.remove(order_col_index);
                    Ok(RecordBatch::try_new(new_schema.clone(), new_columns)?)
                })
                .collect::<Result<Vec<_>>>()?;

            Self::try_new(new_schema, new_batches)
        } else {
            // Not ordering column present, return as-is
            Ok(self)
        }
    }

    pub fn with_ordering(self) -> Result<Self> {
        // Build new schema with leading ORDER_COL
        let mut new_fields = self.schema.fields.to_vec();
        let mut start_idx = 0;
        if new_fields.is_empty() {
            return Ok(Self::empty_with_ordering());
        }

        let order_col_index = self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_i, field)| field.name() == ORDER_COL)
            .map(|(i, _)| i);

        if let Some(order_col_index) = order_col_index {
            new_fields.remove(order_col_index);
        }

        new_fields.insert(0, Arc::new(Field::new(ORDER_COL, ORDER_COL_DTYPE, true)));

        let new_schema = Arc::new(Schema::new(new_fields)) as SchemaRef;

        let new_batches = self
            .batches
            .into_iter()
            .map(|batch| {
                let order_array = Arc::new(UInt32Array::from_iter_values(
                    start_idx..(start_idx + batch.num_rows() as u32),
                )) as ArrayRef;

                let mut new_columns = Vec::from(batch.columns());

                if let Some(order_col_index) = order_col_index {
                    new_columns.remove(order_col_index);
                }

                new_columns.insert(0, order_array);
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
        if let Some(batch) = self.batches.first() {
            schema = batch.schema()
        }
        concat_batches(&schema, &self.batches)
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        if self.num_rows() == 0 {
            // Return empty list with (arbitrary) Float64 type
            let array = Arc::new(new_empty_array(&DataType::Float64));
            return Ok(ScalarValue::List(Arc::new(array_into_list_array(
                array, true,
            ))));
        }
        let array = Arc::new(StructArray::from(self.to_record_batch()?)) as ArrayRef;
        Ok(ScalarValue::List(Arc::new(array_into_list_array(
            array, true,
        ))))
    }

    #[cfg(feature = "json")]
    pub fn to_json(&self) -> Result<serde_json::Value> {
        let mut rows: Vec<serde_json::Value> = Vec::with_capacity(self.num_rows());
        for row in record_batches_to_json_rows(&self.batches)? {
            rows.push(serde_json::Value::Object(row));
        }
        Ok(serde_json::Value::Array(rows))
    }

    #[cfg(feature = "json")]
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        if let serde_json::Value::Array(values) = value {
            // Handle special case where array elements are non-object scalars
            let mut values = Cow::Borrowed(values);
            if let Some(first) = values.first() {
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
                    let array = empty_scalar.to_array_of_size(values.len())?;
                    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                    let record_batch = RecordBatch::from(struct_array);
                    Self::try_new(record_batch.schema(), vec![record_batch])
                }
                Ok(schema) => {
                    let schema_ref = Arc::new(schema);

                    // read record batches
                    let reader =
                        json::ReaderBuilder::new(schema_ref.clone()).with_coerce_primitive(true);
                    let mut decoder = reader.build_decoder()?;

                    let mut batches: Vec<RecordBatch> = Vec::new();
                    decoder.serialize(values.as_slice())?;

                    while let Some(batch) = decoder
                        .flush()
                        .with_context(|| "Failed to read json to arrow")?
                    {
                        batches.push(batch);
                    }

                    Self::try_new(schema_ref, batches)
                }
            }
        } else {
            Err(VegaFusionError::internal(format!(
                "Expected JSON array, not: {value}"
            )))
        }
    }

    #[cfg(feature = "py")]
    pub fn from_pyarrow(py: Python, data: &Bound<PyAny>) -> std::result::Result<Self, PyErr> {
        Ok(Self::from_pyarrow_with_hash(py, data)?.0)
    }

    /// Build a VegaFusion table and hash value suitable for use in VegaFusionDataset
    #[cfg(feature = "py")]
    pub fn from_pyarrow_with_hash(
        py: Python,
        data: &Bound<PyAny>,
    ) -> std::result::Result<(Self, u64), PyErr> {
        let arro3_core = PyModule::import_bound(py, "arro3.core")?;
        let arro3_table_type = arro3_core.getattr("Table")?;
        if data.is_instance(&arro3_table_type)? {
            // Extract original table for single-threaded hashing
            let table = data.extract::<PyTable>().unwrap();
            let (batches, schema) = table.into_inner();
            let vf_table = VegaFusionTable::try_new(schema, batches)?;
            let hash = vf_table.get_hash();

            // Now rechunk for better multithreaded efficiency with DataFusion
            let seq = PyList::new_bound(py, vec![("max_chunksize", 8096)]);
            let kwargs = PyDict::from_sequence_bound(seq.as_any())?;

            let rechunked_table = data
                .call_method("rechunk", (), Some(&kwargs))?
                .extract::<PyTable>()
                .unwrap();
            let (batches, schema) = rechunked_table.into_inner();
            Ok((VegaFusionTable::try_new(schema, batches)?, hash))
        } else {
            // Assume data is a pyarrow Table
            // Extract table.schema as a Rust Schema
            let schema_object = data.getattr("schema")?;
            let pyschema = schema_object.extract::<PySchema>()?;
            let schema = pyschema.into_inner();

            // Extract table.to_batches() as a Rust Vec<RecordBatch>
            let batches_object = data.call_method0("to_batches")?;
            let batches_list = batches_object.downcast::<PyList>()?;
            let batches = batches_list
                .iter()
                .map(|batch_any| Ok(batch_any.extract::<PyRecordBatch>()?.into_inner()))
                .collect::<Result<Vec<RecordBatch>>>()?;

            let vf_table = VegaFusionTable::try_new(schema, batches)?;
            let hash = vf_table.get_hash();
            Ok((vf_table, hash))
        }
    }

    #[cfg(feature = "py")]
    pub fn to_pyo3_arrow(&self) -> std::result::Result<PyTable, PyErr> {
        // Convert table's record batches into Python list of pyarrow batches
        let schema = if let Some(batch) = self.batches.first() {
            // Get schema from first batch if present
            batch.schema()
        } else {
            self.schema.clone()
        };

        let py_table = pyo3_arrow::PyTable::try_new(self.batches.clone(), schema)?;

        Ok(py_table)
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

    #[cfg(feature = "base64")]
    pub fn to_ipc_base64(self) -> Result<String> {
        Ok(general_purpose::STANDARD.encode(self.to_ipc_bytes()?.as_slice()))
    }

    #[cfg(feature = "base64")]
    pub fn from_ipc_base64(data: &str) -> Result<Self> {
        let data = general_purpose::STANDARD.decode(data)?;
        Self::from_ipc_bytes(data.as_slice())
    }

    #[cfg(feature = "prettyprint")]
    pub fn pretty_format(&self, max_rows: Option<usize>) -> Result<String> {
        if let Some(max_rows) = max_rows {
            pretty_format_batches(&self.head(max_rows).batches)
                .with_context(|| String::from("Failed to pretty print"))
                .map(|s| s.to_string())
        } else {
            pretty_format_batches(&self.batches)
                .with_context(|| String::from("Failed to pretty print"))
                .map(|s| s.to_string())
        }
    }

    pub fn get_hash(&self) -> u64 {
        RandomState::with_seed(123).hash_one(self)
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
        // Hash the schema
        self.schema.hash(state);

        // Hash each batch
        for batch in &self.batches {
            // Hash the number of rows in the batch
            batch.num_rows().hash(state);

            // Hash each column in the batch
            for column in batch.columns() {
                hash_array(column, state);
            }
        }
    }
}

fn hash_array<H: Hasher>(array: &ArrayRef, state: &mut H) {
    // Hash the array type
    std::mem::discriminant(array.data_type()).hash(state);

    // Hash the validity bitmap if present
    if let Some(nulls) = array.nulls() {
        nulls.buffer().hash(state);
    }

    match array.data_type() {
        DataType::Null => hash_null_array(array, state),
        DataType::Boolean => array.as_boolean().values().values().hash(state),
        DataType::Int8 => hash_primitive_array::<Int8Type, H>(array, state),
        DataType::Int16 => hash_primitive_array::<Int16Type, H>(array, state),
        DataType::Int32 => hash_primitive_array::<Int32Type, H>(array, state),
        DataType::Int64 => hash_primitive_array::<Int64Type, H>(array, state),
        DataType::UInt8 => hash_primitive_array::<UInt8Type, H>(array, state),
        DataType::UInt16 => hash_primitive_array::<UInt16Type, H>(array, state),
        DataType::UInt32 => hash_primitive_array::<UInt32Type, H>(array, state),
        DataType::UInt64 => hash_primitive_array::<UInt64Type, H>(array, state),
        DataType::Float16 => hash_primitive_array::<Float16Type, H>(array, state),
        DataType::Float32 => hash_primitive_array::<Float32Type, H>(array, state),
        DataType::Float64 => hash_primitive_array::<Float64Type, H>(array, state),
        DataType::Date32 => hash_primitive_array::<Date32Type, H>(array, state),
        DataType::Date64 => hash_primitive_array::<Date64Type, H>(array, state),
        DataType::Time32(TimeUnit::Second) => {
            hash_primitive_array::<Time32SecondType, H>(array, state)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            hash_primitive_array::<Time32MillisecondType, H>(array, state)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            hash_primitive_array::<Time64MicrosecondType, H>(array, state)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            hash_primitive_array::<Time64NanosecondType, H>(array, state)
        }
        DataType::Timestamp(time_unit, tz) => {
            match time_unit {
                TimeUnit::Second => hash_primitive_array::<TimestampSecondType, H>(array, state),
                TimeUnit::Millisecond => {
                    hash_primitive_array::<TimestampMillisecondType, H>(array, state)
                }
                TimeUnit::Microsecond => {
                    hash_primitive_array::<TimestampMicrosecondType, H>(array, state)
                }
                TimeUnit::Nanosecond => {
                    hash_primitive_array::<TimestampNanosecondType, H>(array, state)
                }
            }
            tz.hash(state);
        }
        DataType::Utf8 => hash_string_array::<i32, H>(array, state),
        DataType::LargeUtf8 => hash_string_array::<i64, H>(array, state),
        DataType::Utf8View => hash_string_view_array::<H>(array, state),
        DataType::Binary => hash_binary_array::<i32, H>(array, state),
        DataType::LargeBinary => hash_binary_array::<i64, H>(array, state),
        DataType::BinaryView => hash_binary_view_array::<H>(array, state),
        DataType::Decimal128(a, b) => {
            (*a).hash(state);
            (*b).hash(state);
            hash_primitive_array::<Decimal128Type, H>(array, state);
        }
        DataType::Decimal256(a, b) => {
            (*a).hash(state);
            (*b).hash(state);
            hash_primitive_array::<Decimal256Type, H>(array, state);
        }
        _ => {
            // Fallback that requires cloning the array data
            let array_data = array.to_data();
            hash_array_data(&array_data, state);
        }
    }
}

fn hash_null_array<H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array.as_any().downcast_ref::<NullArray>().unwrap();
    if let Some(nulls) = array.logical_nulls() {
        nulls.buffer().hash(state);
    }
}

fn hash_primitive_array<T: ArrowPrimitiveType, H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    array.values().to_byte_slice().hash(state);
}

fn hash_string_array<S: OffsetSizeTrait, H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array
        .as_any()
        .downcast_ref::<GenericStringArray<S>>()
        .unwrap();
    array.value_offsets().to_byte_slice().hash(state);
    array.value_data().to_byte_slice().hash(state);
}

fn hash_string_view_array<H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();

    // Hash view buffer
    array.views().hash(state);

    // Hash data buffers
    for buffer in array.data_buffers() {
        buffer.hash(state);
    }
}

fn hash_binary_array<S: OffsetSizeTrait, H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array
        .as_any()
        .downcast_ref::<GenericBinaryArray<S>>()
        .unwrap();
    array.value_offsets().to_byte_slice().hash(state);
    array.value_data().to_byte_slice().hash(state);
}

fn hash_binary_view_array<H: Hasher>(array: &ArrayRef, state: &mut H) {
    let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();

    // Hash view buffer
    array.views().hash(state);

    // Hash data buffers
    for buffer in array.data_buffers() {
        buffer.hash(state);
    }
}

fn hash_array_data<H: Hasher>(array_data: &ArrayData, state: &mut H) {
    for buffer in array_data.buffers() {
        buffer.hash(state);
    }

    // For nested types (list, struct), recursively hash child arrays
    let child_data = array_data.child_data();
    for child in child_data {
        hash_array_data(child, state);
    }
}

#[cfg(feature = "json")]
#[cfg(test)]
mod tests {
    use crate::data::table::VegaFusionTable;
    use serde_json::json;

    #[test]
    fn test_with_ordering() {
        let table1 = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": "A"},
            {"a": 2, "b": "BB"},
            {"a": 10, "b": "CCC"},
            {"a": 20, "b": "DDDD"},
        ]))
        .unwrap();
        assert_eq!(table1.batches.len(), 1);

        let table2 = VegaFusionTable::from_json(&json!([
            {"_vf_order": 10u32, "a": 1, "b": "A"},
            {"_vf_order": 9u32, "a": 2, "b": "BB"},
            {"_vf_order": 8u32, "a": 10, "b": "CCC"},
            {"_vf_order": 7u32, "a": 20, "b": "DDDD"},
        ]))
        .unwrap();
        assert_eq!(table2.batches.len(), 1);

        let expected_json = json!([
            {"_vf_order": 0u32, "a": 1, "b": "A"},
            {"_vf_order": 1u32, "a": 2, "b": "BB"},
            {"_vf_order": 2u32, "a": 10, "b": "CCC"},
            {"_vf_order": 3u32, "a": 20, "b": "DDDD"},
        ]);

        // Add ordering column to table without one
        let result_table1 = table1.with_ordering().unwrap();
        assert_eq!(result_table1.batches.len(), 1);
        assert_eq!(result_table1.to_json().unwrap(), expected_json);

        // Override prior ordering column
        let result_table2 = table2.with_ordering().unwrap();
        assert_eq!(result_table2.batches.len(), 1);
        assert_eq!(result_table2.to_json().unwrap(), expected_json);
    }
}
