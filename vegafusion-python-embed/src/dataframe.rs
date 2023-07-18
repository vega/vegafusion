use arrow::datatypes::Schema;
use arrow::pyarrow::FromPyArrow;
use async_trait::async_trait;
use datafusion_proto::protobuf::LogicalExprNode;
use prost::Message;
use pyo3::prelude::PyModule;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::{pyclass, pymethods, IntoPy, PyErr, PyObject, Python};
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::Expr;
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
        // the identical. This is the conservative approach, and later on we can revisit how
        // to configure this as an optimization.
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

    async fn sort(&self, exprs: Vec<Expr>, limit: Option<i32>) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let py_exprs = exprs_to_py(py, exprs.clone())?;

            // Convert limit to PyObject
            let py_limit = limit.into_py(py);

            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_exprs, py_limit]);

            let new_py_df = match self.dataframe.call_method(py, "sort", args, None) {
                Ok(new_py_df) => new_py_df,
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;
                    if exception_name == "DataFrameOperationNotSupportedError" {
                        // Should fall back to fallback connection below
                        return Ok(None);
                    }
                    // Propagate other exceptions
                    return Err(err);
                }
            };

            Ok(Some(Arc::new(PyDataFrame {
                dataframe: new_py_df,
                fallback_conn: self.fallback_conn.clone(),
            })))
        })?;

        if let Some(new_df) = new_df {
            Ok(new_df)
        } else {
            // Fallback
            let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
                let table = self.dataframe.call_method0(py, "collect")?;
                VegaFusionTable::from_pyarrow(py, table.as_ref(py))
            })?;

            let new_df: Arc<dyn DataFrame> = self.fallback_conn.scan_arrow(table).await?;

            println!("Fallback sort: {:?}", exprs);
            new_df.sort(exprs, limit).await
        }
    }

    async fn select(&self, exprs: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let py_exprs = exprs_to_py(py, exprs.clone())?;
            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_exprs]);

            let new_py_df = match self.dataframe.call_method(py, "select", args, None) {
                Ok(new_py_df) => new_py_df,
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;
                    if exception_name == "DataFrameOperationNotSupportedError" {
                        // Should fall back to fallback connection below
                        println!("Fall back form select");
                        return Ok(None);
                    }
                    // Propagate other exceptions
                    return Err(err);
                }
            };

            Ok(Some(Arc::new(PyDataFrame {
                dataframe: new_py_df,
                fallback_conn: self.fallback_conn.clone(),
            })))
        })?;

        if let Some(new_df) = new_df {
            Ok(new_df)
        } else {
            // Fallback
            let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
                let table = self.dataframe.call_method0(py, "collect")?;
                VegaFusionTable::from_pyarrow(py, table.as_ref(py))
            })?;

            let new_df: Arc<dyn DataFrame> = self.fallback_conn.scan_arrow(table).await?;
            println!("Fallback select: {:?}", exprs);
            new_df.select(exprs).await
        }
    }

    async fn filter(&self, predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let py_exprs = expr_to_py(py, &predicate)?;
            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_exprs]);

            let new_py_df = match self.dataframe.call_method(py, "filter", args, None) {
                Ok(new_py_df) => new_py_df,
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;
                    if exception_name == "DataFrameOperationNotSupportedError" {
                        // Should fall back to fallback connection below
                        return Ok(None);
                    }
                    // Propagate other exceptions
                    return Err(err);
                }
            };

            Ok(Some(Arc::new(PyDataFrame {
                dataframe: new_py_df,
                fallback_conn: self.fallback_conn.clone(),
            })))
        })?;

        if let Some(new_df) = new_df {
            Ok(new_df)
        } else {
            // Fallback
            let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
                let table = self.dataframe.call_method0(py, "collect")?;
                VegaFusionTable::from_pyarrow(py, table.as_ref(py))
            })?;

            let new_df: Arc<dyn DataFrame> = self.fallback_conn.scan_arrow(table).await?;
            println!("Fallback filter: {:?}", predicate);
            new_df.filter(predicate).await
        }
    }
}

fn exprs_to_py(py: Python, exprs: Vec<Expr>) -> Result<PyObject> {
    let py_exprs: Vec<PyObject> = exprs
        .iter()
        .map(|expr| expr_to_py(py, expr))
        .collect::<Result<Vec<PyObject>>>()?;
    let py_exprs = py_exprs.into_py(py);
    Ok(py_exprs)
}

fn expr_to_py(py: Python, expr: &Expr) -> Result<PyObject> {
    let proto_module = PyModule::import(py, "vegafusion.proto.datafusion_pb2")?;
    let logical_expr_class = proto_module.getattr("LogicalExprNode")?;

    let proto_sort_expr = LogicalExprNode::try_from(expr)?;
    let sort_expr_bytes: Vec<u8> = proto_sort_expr.encode_to_vec();

    // py_logical_expr = LogicalExprNode()
    let py_logical_expr = logical_expr_class.call(PyTuple::empty(py), None)?;

    // py_logical_expr.ParseFromString(sort_expr_bytes)
    let py_bytes = PyBytes::new(py, sort_expr_bytes.as_slice());
    let args = PyTuple::new(py, vec![py_bytes]);
    py_logical_expr.call_method("ParseFromString", args, None)?;

    // From &PyAny to PyObject to maintain ownership
    Ok(py_logical_expr.into_py(py))
}
