use pyo3::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use async_trait::async_trait;
use pyo3::types::{IntoPyDict, PyDict, PyString, PyTuple};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::{arrow::datatypes::Schema, error::Result};
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use vegafusion_sql::connection::{Connection, SqlConnection};
use vegafusion_sql::dataframe::{CsvReadOptions, DataFrame, SqlDataFrame};
use vegafusion_sql::dialect::Dialect;

fn get_dialect_and_fallback_connection(
    conn: &PyObject,
) -> Result<(Dialect, Option<Arc<dyn SqlConnection>>)> {
    let mut dialect = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
        let dialect_object = conn.call_method0(py, "dialect")?;
        let dialect_string = dialect_object.extract::<String>(py)?;
        Ok(Dialect::from_str(&dialect_string)?)
    })?;

    let fallback_conn = Python::with_gil(
        |py| -> std::result::Result<Option<Arc<dyn SqlConnection>>, PyErr> {
            let should_fallback_object = conn.call_method0(py, "fallback")?;
            let should_fallback = should_fallback_object.extract::<bool>(py)?;
            if should_fallback {
                // Create fallback DataFusion connection. This will be used when SQL is encountered
                // that isn't supported by the main connection.
                let fallback_conn: DataFusionConnection = Default::default();
                Ok(Some(Arc::new(fallback_conn)))
            } else {
                Ok(None)
            }
        },
    )?;

    if fallback_conn.is_some() {
        // If we are going to fall back to the DataFusion connection, remove the
        // str_to_utc_timestamp scalar function so that timestamp parsing falls back to
        // our DataFusion UDF, which matches Vega more closely than external SQL engines.
        dialect.scalar_functions.remove("str_to_utc_timestamp");
        dialect.scalar_transformers.remove("str_to_utc_timestamp");
    }
    Ok((dialect, fallback_conn))
}

fn perform_fetch_query(query: &str, schema: &Schema, conn: &PyObject) -> Result<VegaFusionTable> {
    let table = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
        let query_object = PyString::new(py, query);
        let query_object = query_object.as_ref();
        let schema_object = schema.to_pyarrow(py)?;
        let schema_object = schema_object.as_ref(py);
        let args = PyTuple::new(py, vec![query_object, schema_object]);
        let table_object = conn.call_method(py, "fetch_query", args, None)?;
        VegaFusionTable::from_pyarrow(py, table_object.as_ref(py))
    })?;
    Ok(table)
}

#[pyclass]
#[derive(Clone)]
pub struct PySqlConnection {
    conn: PyObject,
    dialect: Dialect,
    fallback_conn: Option<Arc<dyn SqlConnection>>,
}

#[pymethods]
impl PySqlConnection {
    #[new]
    pub fn new(conn: PyObject) -> Result<Self> {
        let (dialect, fallback_conn) = get_dialect_and_fallback_connection(&conn)?;

        Ok(Self {
            conn,
            dialect,
            fallback_conn,
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

    /// Scan a named table into a DataFrame
    async fn scan_table(&self, name: &str) -> Result<Arc<dyn DataFrame>> {
        // Build DataFrame referencing the registered table
        Ok(Arc::new(
            SqlDataFrame::try_new(
                Arc::new(self.clone()),
                name,
                self.fallback_conn.clone().into_iter().collect(),
            )
            .await?,
        ))
    }

    /// Scan a VegaFusionTable into a DataFrame
    async fn scan_arrow(&self, table: VegaFusionTable) -> Result<Arc<dyn DataFrame>> {
        let random_id = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let table_name = format!("arrow_{random_id}");
        let fallback_connection = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let pa_table = table.to_pyarrow(py)?;

            // Register table with Python connection
            let table_name_object = table_name.clone().into_py(py);
            let is_temporary_object = true.into_py(py);
            let args = PyTuple::new(py, vec![table_name_object, pa_table, is_temporary_object]);

            match self.conn.call_method1(py, "register_arrow", args) {
                Ok(_) => {}
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;

                    // Check if we have a fallback connection and this is a RegistrationNotSupportedError
                    if let Some(fallback_connection) = &self.fallback_conn {
                        if exception_name == "RegistrationNotSupportedError" {
                            return Ok(Some(fallback_connection));
                        }
                    }
                    return Err(err);
                }
            }
            Ok(None)
        })?;

        if let Some(fallback_connection) = fallback_connection {
            // Try again with fallback connection
            fallback_connection.scan_arrow(table).await
        } else {
            // Build DataFrame referencing the registered table
            Ok(Arc::new(
                SqlDataFrame::try_new(
                    Arc::new(self.clone()),
                    &table_name,
                    self.fallback_conn.clone().into_iter().collect(),
                )
                .await?,
            ))
        }
    }

    async fn scan_csv(&self, url: &str, opts: CsvReadOptions) -> Result<Arc<dyn DataFrame>> {
        let random_id = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let table_name = format!("csv_{random_id}");

        let inner_opts = opts.clone();
        let fallback_connection = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Build Python CsvReadOptions
            let vegafusion_module = PyModule::import(py, "vegafusion.connection")?;
            let csv_opts_class = vegafusion_module.getattr("CsvReadOptions")?;

            let pyschema = inner_opts
                .schema
                .and_then(|schema| schema.to_pyarrow(py).ok())
                .into_py(py);
            let kwargs = vec![
                ("has_header", inner_opts.has_header.into_py(py)),
                (
                    "delimeter",
                    (inner_opts.delimiter as char).to_string().into_py(py),
                ),
                ("file_extension", inner_opts.file_extension.into_py(py)),
                ("schema", pyschema),
            ]
            .into_py_dict(py);
            let args = PyTuple::empty(py);
            let csv_opts = csv_opts_class.call(args, Some(kwargs))?;

            // Register table with Python connection
            let table_name_object = table_name.clone().into_py(py);
            let path_name_object = url.to_string().into_py(py);
            let is_temporary_object = true.into_py(py);
            let args = PyTuple::new(
                py,
                vec![
                    table_name_object.as_ref(py),
                    path_name_object.as_ref(py),
                    csv_opts,
                    is_temporary_object.as_ref(py),
                ],
            );

            match self.conn.call_method1(py, "register_csv", args) {
                Ok(_) => {}
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;

                    // Check if we have a fallback connection and this is a RegistrationNotSupportedError
                    if let Some(fallback_connection) = &self.fallback_conn {
                        if exception_name == "RegistrationNotSupportedError" {
                            return Ok(Some(fallback_connection));
                        }
                    }
                    return Err(err);
                }
            }
            Ok(None)
        })?;

        if let Some(fallback_connection) = fallback_connection {
            // Try again with fallback connection
            fallback_connection.scan_csv(url, opts).await
        } else {
            // Build DataFrame referencing the registered table
            Ok(Arc::new(
                SqlDataFrame::try_new(
                    Arc::new(self.clone()),
                    &table_name,
                    self.fallback_conn.clone().into_iter().collect(),
                )
                .await?,
            ))
        }
    }

    async fn scan_arrow_file(&self, path: &str) -> Result<Arc<dyn DataFrame>> {
        let random_id = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let table_name = format!("arrow_file_{random_id}");

        let fallback_connection = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Register table with Python connection
            let table_name_object = table_name.clone().into_py(py);
            let path_name_object = path.to_string().into_py(py);
            let is_temporary_object = true.into_py(py);

            let args = PyTuple::new(
                py,
                vec![
                    table_name_object.as_ref(py),
                    path_name_object.as_ref(py),
                    is_temporary_object.as_ref(py),
                ],
            );
            match self.conn.call_method1(py, "register_arrow_file", args) {
                Ok(_) => {}
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;

                    // Check if we have a fallback connection and this is a RegistrationNotSupportedError
                    if let Some(fallback_connection) = &self.fallback_conn {
                        if exception_name == "RegistrationNotSupportedError" {
                            return Ok(Some(fallback_connection));
                        }
                    }
                    return Err(err);
                }
            }
            Ok(None)
        })?;

        if let Some(fallback_connection) = fallback_connection {
            // Try again with fallback connection
            fallback_connection.scan_arrow_file(path).await
        } else {
            // Build DataFrame referencing the registered table
            Ok(Arc::new(
                SqlDataFrame::try_new(
                    Arc::new(self.clone()),
                    &table_name,
                    self.fallback_conn.clone().into_iter().collect(),
                )
                .await?,
            ))
        }
    }

    async fn scan_parquet(&self, path: &str) -> Result<Arc<dyn DataFrame>> {
        let random_id = uuid::Uuid::new_v4().to_string().replace('-', "_");
        let table_name = format!("parquet_{random_id}");

        let fallback_connection = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            // Register table with Python connection
            let table_name_object = table_name.clone().into_py(py);
            let path_name_object = path.to_string().into_py(py);
            let is_temporary_object = true.into_py(py);

            let args = PyTuple::new(
                py,
                vec![
                    table_name_object.as_ref(py),
                    path_name_object.as_ref(py),
                    is_temporary_object.as_ref(py),
                ],
            );
            match self.conn.call_method1(py, "register_parquet", args) {
                Ok(_) => {}
                Err(err) => {
                    let exception_name = err.get_type(py).name()?;

                    // Check if we have a fallback connection and this is a RegistrationNotSupportedError
                    if let Some(fallback_connection) = &self.fallback_conn {
                        if exception_name == "RegistrationNotSupportedError" {
                            return Ok(Some(fallback_connection));
                        }
                    }
                    return Err(err);
                }
            }
            Ok(None)
        })?;

        if let Some(fallback_connection) = fallback_connection {
            // Try again with fallback connection
            fallback_connection.scan_parquet(path).await
        } else {
            // Build DataFrame referencing the registered table
            Ok(Arc::new(
                SqlDataFrame::try_new(
                    Arc::new(self.clone()),
                    &table_name,
                    self.fallback_conn.clone().into_iter().collect(),
                )
                .await?,
            ))
        }
    }
}

#[async_trait]
impl SqlConnection for PySqlConnection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable> {
        perform_fetch_query(query, schema, &self.conn)
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    fn to_connection(&self) -> Arc<dyn Connection> {
        Arc::new(self.clone())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PySqlDataset {
    pub dataset: PyObject,
    pub dialect: Dialect,
    pub table_name: String,
    pub table_schema: Schema,
    pub fallback_conn: Option<Arc<dyn SqlConnection>>,
}

#[pymethods]
impl PySqlDataset {
    #[new]
    pub fn new(dataset: PyObject) -> Result<Self> {
        let (dialect, fallback_conn) = get_dialect_and_fallback_connection(&dataset)?;
        let (table_name, table_schema) = Python::with_gil(|py| -> std::result::Result<_, PyErr> {
            let table_name_obj = dataset.call_method0(py, "table_name")?;
            let table_name = table_name_obj.extract::<String>(py)?;

            let table_schema_obj = dataset.call_method0(py, "table_schema")?;
            let table_schema = Schema::from_pyarrow(table_schema_obj.as_ref(py))?;
            Ok((table_name, table_schema))
        })?;

        Ok(Self {
            dataset,
            dialect,
            table_name,
            table_schema,
            fallback_conn,
        })
    }
}

#[async_trait]
impl Connection for PySqlDataset {
    fn id(&self) -> String {
        // Include random UUID in id because we can't be sure that the underlying data source
        // hasn't changed between calls.
        format!("pyduckdb-{}", uuid::Uuid::new_v4())
    }

    async fn tables(&self) -> Result<HashMap<String, Schema>> {
        Ok(vec![(self.table_name.clone(), self.table_schema.clone())]
            .into_iter()
            .collect())
    }

    async fn scan_table(&self, name: &str) -> Result<Arc<dyn DataFrame>> {
        // Build DataFrame referencing the registered table
        Ok(Arc::new(
            SqlDataFrame::try_new(
                Arc::new(self.clone()),
                name,
                self.fallback_conn.clone().into_iter().collect(),
            )
            .await?,
        ))
    }
}

#[async_trait]
impl SqlConnection for PySqlDataset {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable> {
        perform_fetch_query(query, schema, &self.dataset)
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    fn to_connection(&self) -> Arc<dyn Connection> {
        Arc::new(self.clone())
    }
}
