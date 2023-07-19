use arrow::datatypes::Schema;
use arrow::pyarrow::FromPyArrow;
use async_trait::async_trait;
use datafusion_proto::protobuf::LogicalExprNode;
use prost::Message;
use pyo3::prelude::PyModule;
use pyo3::types::{PyBytes, PyTuple};
use pyo3::{pyclass, pymethods, IntoPy, PyErr, PyObject, Python};
use serde_json::Value;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_common::ScalarValue;
use vegafusion_common::datafusion_expr::Expr;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_dataframe::connection::Connection;
use vegafusion_dataframe::dataframe::{DataFrame, StackMode};
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

            new_df.select(exprs).await
        }
    }

    async fn aggregate(
        &self,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let py_group_exprs = exprs_to_py(py, group_exprs.clone())?;
            let py_aggr_exprs = exprs_to_py(py, aggr_exprs.clone())?;

            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_group_exprs, py_aggr_exprs]);

            let new_py_df = match self.dataframe.call_method(py, "aggregate", args, None) {
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

            new_df.aggregate(group_exprs, aggr_exprs).await
        }
    }

    async fn joinaggregate(
        &self,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let py_group_exprs = exprs_to_py(py, group_exprs.clone())?;
            let py_aggr_exprs = exprs_to_py(py, aggr_exprs.clone())?;

            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_group_exprs, py_aggr_exprs]);

            let new_py_df = match self.dataframe.call_method(py, "joinaggregate", args, None) {
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

            new_df.joinaggregate(group_exprs, aggr_exprs).await
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

            new_df.filter(predicate).await
        }
    }

    async fn limit(&self, limit: i32) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Convert limit to PyObject
            let py_limit = limit.into_py(py);

            // Build arguments for Python sort method
            let args = PyTuple::new(py, vec![py_limit]);

            let new_py_df = match self.dataframe.call_method(py, "limit", args, None) {
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

            new_df.limit(limit).await
        }
    }

    async fn fold(
        &self,
        fields: &[String],
        value_col: &str,
        key_col: &str,
        order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Convert limit to PyObject
            let py_fields = Vec::from(fields).into_py(py);
            let py_value_col = value_col.into_py(py);
            let py_key_col = key_col.into_py(py);
            let py_order_field = order_field.into_py(py);

            // Build arguments for Python sort method
            let args = PyTuple::new(
                py,
                vec![py_fields, py_value_col, py_key_col, py_order_field],
            );

            let new_py_df = match self.dataframe.call_method(py, "fold", args, None) {
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

            new_df.fold(fields, value_col, key_col, order_field).await
        }
    }

    async fn stack(
        &self,
        field: &str,
        orderby: Vec<Expr>,
        groupby: &[String],
        start_field: &str,
        stop_field: &str,
        mode: StackMode,
    ) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Convert limit to PyObject
            let py_field = field.into_py(py);
            let py_orderby = exprs_to_py(py, orderby.clone())?;
            let py_groupby = Vec::from(groupby).into_py(py);
            let py_start_field = start_field.into_py(py);
            let py_stop_field = stop_field.into_py(py);
            let py_mode = mode.to_string().to_ascii_lowercase().into_py(py);

            // Build arguments for Python sort method
            let args = PyTuple::new(
                py,
                vec![
                    py_field,
                    py_orderby,
                    py_groupby,
                    py_start_field,
                    py_stop_field,
                    py_mode,
                ],
            );

            let new_py_df = match self.dataframe.call_method(py, "stack", args, None) {
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

            new_df
                .stack(field, orderby, groupby, start_field, stop_field, mode)
                .await
        }
    }

    async fn impute(
        &self,
        field: &str,
        value: ScalarValue,
        key: &str,
        groupby: &[String],
        order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        let new_df = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Convert limit to PyObject
            let py_field = field.into_py(py);
            let py_value = scalar_value_to_py(py, &value)?;
            let py_key = key.into_py(py);
            let py_groupby = Vec::from(groupby).into_py(py);
            let py_order_field = order_field.into_py(py);

            // Build arguments for Python sort method
            let args = PyTuple::new(
                py,
                vec![py_field, py_value, py_key, py_groupby, py_order_field],
            );

            let new_py_df = match self.dataframe.call_method(py, "impute", args, None) {
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

            new_df.impute(field, value, key, groupby, order_field).await
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

fn scalar_value_to_py(py: Python, value: &ScalarValue) -> Result<PyObject> {
    Ok(match value.to_json()? {
        Value::Null => Python::None(py),
        Value::Bool(v) => v.into_py(py),
        Value::Number(v) => {
            if v.is_i64() {
                v.as_i64().into_py(py)
            } else if v.is_u64() {
                v.as_u64().into_py(py)
            } else {
                v.as_f64().into_py(py)
            }
        }
        Value::String(v) => v.into_py(py),
        _ => {
            return Err(VegaFusionError::internal(format!(
                "Unsupported value for conversion to Python: {:?}",
                value
            )))
        }
    })
}
