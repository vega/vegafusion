use datafusion_common::ScalarValue;

use arrow::{
    array::{ArrayRef, StructArray, UInt32Array},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};

use crate::{
    data::{ORDER_COL, ORDER_COL_DTYPE},
    error::{Result, ResultWithContext, VegaFusionError},
};

#[cfg(feature = "prettyprint")]
use arrow::util::pretty::pretty_format_batches;
use std::{
    hash::{Hash, Hasher},
    io::Cursor,
    sync::Arc,
};

#[cfg(feature = "json")]
use {
    crate::data::json_writer::record_batches_to_json_rows,
    arrow::json,
    serde_json::{json, Value},
    std::{borrow::Cow, convert::TryFrom},
};

#[cfg(feature = "pyarrow")]
use {
    arrow::pyarrow::{FromPyArrow, ToPyArrow},
    pyo3::{
        prelude::PyModule,
        types::{PyList, PyTuple},
        PyAny, PyErr, PyObject, Python,
    },
};

#[cfg(feature = "base64")]
use base64::{engine::general_purpose, Engine as _};

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
        if partitions.iter().all(|batches| {
            let batch_schema_fields: Vec<_> = batches
                .schema()
                .fields
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect();
            let batch_schema = Arc::new(Schema::new(batch_schema_fields));
            schema.contains(&batch_schema)
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
                Arc::new(Field::new("item", dtype, true)),
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

        let dtype = elements[0].data_type();
        Ok(ScalarValue::List(
            Some(elements),
            Arc::new(Field::new("item", dtype, true)),
        ))
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

    #[cfg(feature = "pyarrow")]
    pub fn from_pyarrow(py: Python, pyarrow_table: &PyAny) -> std::result::Result<Self, PyErr> {
        // Extract table.schema as a Rust Schema
        let getattr_args = PyTuple::new(py, vec!["schema"]);
        let schema_object = pyarrow_table.call_method1("__getattribute__", getattr_args)?;
        let schema = Schema::from_pyarrow(schema_object)?;

        // Extract table.to_batches() as a Rust Vec<RecordBatch>
        let batches_object = pyarrow_table.call_method0("to_batches")?;
        let batches_list = batches_object.downcast::<PyList>()?;
        let batches = batches_list
            .iter()
            .map(|batch_any| Ok(RecordBatch::from_pyarrow(batch_any)?))
            .collect::<Result<Vec<RecordBatch>>>()?;

        Ok(VegaFusionTable::try_new(Arc::new(schema), batches)?)
    }

    #[cfg(feature = "pyarrow")]
    pub fn to_pyarrow(&self, py: Python) -> std::result::Result<PyObject, PyErr> {
        // Convert table's record batches into Python list of pyarrow batches
        let pyarrow_module = PyModule::import(py, "pyarrow")?;
        let table_cls = pyarrow_module.getattr("Table")?;
        let batch_objects = self
            .batches
            .iter()
            .map(|batch| Ok(batch.to_pyarrow(py)?))
            .collect::<Result<Vec<_>>>()?;
        let batches_list = PyList::new(py, batch_objects);

        // Convert table's schema into pyarrow schema
        let schema = if let Some(batch) = self.batches.get(0) {
            // Get schema from first batch if present
            batch.schema()
        } else {
            self.schema.clone()
        };

        let schema_object = schema.to_pyarrow(py)?;

        // Build pyarrow table
        let args = PyTuple::new(py, vec![batches_list.as_ref(), schema_object.as_ref(py)]);
        let pa_table = table_cls.call_method1("from_batches", args)?;
        Ok(PyObject::from(pa_table))
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
