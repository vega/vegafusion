use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use pyo3::prelude::*;

use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::{
    error::Result,
    arrow::{
        datatypes::Schema,
        record_batch::RecordBatch,
    },
};
use vegafusion_sql::connection::{SqlConnection, Connection};
use vegafusion_sql::dialect::Dialect;
use async_trait::async_trait;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use arrow::pyarrow::{PyArrowConvert};
use vegafusion_sql::dataframe::{DataFrame, SqlDataFrame};

#[pyclass]
#[derive(Clone)]
pub struct PySqlConnection {
    conn: PyObject,
    dialect: Dialect,
}

#[pymethods]
impl PySqlConnection {
    #[new]
    pub fn new(conn: PyObject) -> Result<Self> {
        let dialect = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let dialect_object = conn.call_method0(py, "dialect")?;
            let dialect_string = dialect_object.extract::<String>(py)?;
            Ok(Dialect::from_str(&dialect_string)?)
        })?;

        Ok(Self {
            conn,
            dialect,
        })
    }
}

#[async_trait]
impl Connection for PySqlConnection {
    fn id(&self) -> String {
        "pyduckdb".to_string()
    }

    async fn tables(&self) -> Result<HashMap<String, Schema>> {
        let tables = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let tables_object = self.conn.call_method0(py, "tables")?;
            let tables_dict = tables_object.downcast::<PyDict>(py)?;

            let mut tables: HashMap<String, Schema> = HashMap::new();

            for key in tables_dict.keys() {
                let value = tables_dict.get_item(key).unwrap();
                let key_string = key.extract::<String>()?;
                let value_schema = Schema::from_pyarrow(value)?;
                tables.insert(key_string, value_schema);
            }
            Ok(tables)
        })?;

        Ok(tables)
    }

    /// Scan a VegaFusionTable into a DataFrame
    async fn scan_arrow(&self, table: VegaFusionTable) -> Result<Arc<dyn DataFrame>> {
        Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Convert table's record batches into Python list of pyarrow batches
            let pyarrow_module = PyModule::import(py, "pyarrow")?;
            let table_cls = pyarrow_module.getattr("Table")?;
            let batch_objects = table.batches.iter().map(|batch| {
                Ok(batch.to_pyarrow(py)?)
            }).collect::<Result<Vec<_>>>()?;
            let batches_list = PyList::new(py, batch_objects);

            // Convert table's schema into pyarrow schema
            let schema = if let Some(batch) = table.batches.get(0) {
                // Get schema from first batch if present
                batch.schema()
            } else {
                table.schema.clone()
            };

            let schema_object = schema.to_pyarrow(py)?;

            // Build pyarrow table
            let args = PyTuple::new(py, vec![
                batches_list.as_ref(), schema_object.as_ref(py)
            ]);
            let pa_table = table_cls.call_method1("from_batches", args)?;

            // Register table with Python connection
            let table_name_object = "tbl".to_string().into_py(py);
            let args = PyTuple::new(py, vec![
                table_name_object.as_ref(py), pa_table
            ]);
            self.conn.call_method1(py, "register_arrow", args)?;
            Ok(())
        })?;

        // Build DataFrame referencing the registered table
        Ok(Arc::new(SqlDataFrame::try_new(Arc::new(self.clone()), "tbl").await?))
    }
}

#[async_trait]
impl SqlConnection for PySqlConnection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable> {
        let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let query_object = PyString::new(py, query);
            let query_object = query_object.as_ref();

            let schema_object = schema.to_pyarrow(py)?;
            let schema_object = schema_object.as_ref(py);
            let args = PyTuple::new(py, vec![query_object, schema_object]);

            let table_object = self.conn.call_method(py, "fetch_query", args, None)?;

            // Extract table.schema as a Rust Schema
            let getattr_args = PyTuple::new(py, vec!["schema"]);
            let schema_object = table_object.call_method1(py, "__getattribute__", getattr_args)?;
            let schema = Schema::from_pyarrow(schema_object.as_ref(py))?;

            // Extract table.to_batches() as a Rust Vec<RecordBatch>
            let batches_object = table_object.call_method0(py, "to_batches")?;
            let batches_list = batches_object.downcast::<PyList>(py)?;
            let batches = batches_list.iter().map(|batch_any| {
                Ok(RecordBatch::from_pyarrow(batch_any)?)
            }).collect::<Result<Vec<RecordBatch>>>()?;

            Ok(VegaFusionTable::try_new(Arc::new(schema), batches)?)
        })?;
        Ok(table)
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    fn to_connection(&self) -> Arc<dyn Connection> {
        Arc::new(self.clone())
    }
}
