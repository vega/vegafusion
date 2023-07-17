use arrow::datatypes::Schema;
use arrow::pyarrow::FromPyArrow;
use async_trait::async_trait;
use pyo3::{pyclass, pymethods, PyErr, PyObject, Python};
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::Result;
use vegafusion_dataframe::connection::Connection;
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

#[pyclass]
#[derive(Clone)]
pub struct PyDataFrame {
    dataframe: PyObject,
    fallback_conn: Arc<dyn Connection>,
}

#[pymethods]
impl PyDataFrame {
    #[new]
    pub fn new(dataframe: PyObject) -> Result<Self> {
        Ok(Self {
            dataframe,
            fallback_conn: Arc::new(DataFusionConnection::default()),
        })
    }
}

#[async_trait]
impl DataFrame for PyDataFrame {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> Schema {
        Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let schema_obj = self.dataframe.call_method0(py, "schema")?;
            let schema = Schema::from_pyarrow(schema_obj.as_ref(py))?;
            Ok(schema)
        })
        .expect("Failed to return Schema of DataFrameDatasource")
    }

    fn connection(&self) -> Arc<dyn Connection> {
        self.fallback_conn.clone()
    }

    fn fingerprint(&self) -> u64 {
        // Use a random fingerprint for now to not assume that repeated evaluations will be
        // the same.
        let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());
        let rand_uuid = uuid::Uuid::new_v4().to_string();
        rand_uuid.hash(&mut hasher);

        hasher.finish()
    }

    async fn collect(&self) -> Result<VegaFusionTable> {
        let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let table_object = self.dataframe.call_method0(py, "collect")?;
            VegaFusionTable::from_pyarrow(py, table_object.as_ref(py))
        })?;
        Ok(table)
    }
}
