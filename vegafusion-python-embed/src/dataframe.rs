use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::Schema;
use pyo3::{PyErr, PyObject, Python};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::connection::SqlConnection;
use vegafusion_common::error::Result;
use vegafusion_dataframe::connection::Connection;
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use vegafusion_dataframe::dataframe::DataFrame;

#[pyclass]
#[derive(Clone)]
pub struct PyDataFrame {
    dataframe: PyObject,
    fallback_conn: Arc<dyn SqlConnection>,
}


#[pymethods]
impl PyDataFrame {
    #[new]
    pub fn new(dataframe: PyObject) -> Result<Self> {
        Ok(Self {
            dataframe,
            fallback_conn: Arc::new(DataFusionConnection = Default::default()),
        })
    }
}

impl DataFrame for PyDataFrame {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> Schema {
        todo!()
    }

    fn connection(&self) -> Arc<dyn Connection> {
        self.fallback_conn.clone()
    }

    fn fingerprint(&self) -> u64 {
        todo!()
    }

    async fn collect(&self) -> Result<VegaFusionTable> {
        todo!()
    }
}