pub mod connection;
pub mod dataframe;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::collections::HashMap;
use std::sync::{Arc, Once};
use tokio::runtime::Runtime;
use vegafusion_core::error::{ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::pretransform::pre_transform_extract_warning::WarningType as ExtractWarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValueWarningType;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

use crate::connection::{PySqlConnection, PySqlDataset};
use crate::dataframe::PyDataFrame;
use env_logger::{Builder, Target};
use pythonize::depythonize;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::patch::patch_pre_transformed_spec;
use vegafusion_core::planning::plan::PreTransformSpecWarningSpec;
use vegafusion_core::proto::gen::tasks::Variable;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_runtime::data::dataset::VegaFusionDataset;
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use vegafusion_sql::connection::Connection;

static INIT: Once = Once::new();

pub fn initialize_logging() {
    INIT.call_once(|| {
        // Init logger to write to stdout
        let mut builder = Builder::from_default_env();
        builder.target(Target::Stdout);
        builder.init();
    });
}

#[pyclass]
struct PyVegaFusionRuntime {
    runtime: VegaFusionRuntime,
    tokio_runtime_connection: Arc<Runtime>,
    tokio_runtime_current_thread: Arc<Runtime>,
}

impl PyVegaFusionRuntime {
    fn process_inline_datasets(
        &self,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<(HashMap<String, VegaFusionDataset>, bool)> {
        let mut any_python_sources = false;
        if let Some(inline_datasets) = inline_datasets {
            Python::with_gil(|py| -> PyResult<_> {
                let vegafusion_dataset_module = PyModule::import(py, "vegafusion.dataset")?;
                let sql_dataset_type = vegafusion_dataset_module.getattr("SqlDataset")?;
                let df_dataset_type = vegafusion_dataset_module.getattr("DataFrameDataset")?;

                let imported_datasets = inline_datasets
                    .iter()
                    .map(|(name, inline_dataset)| {
                        let dataset = if inline_dataset.is_instance(sql_dataset_type)? {
                            any_python_sources = true;
                            let sql_dataset = PySqlDataset::new(inline_dataset.into_py(py))?;
                            let df = self
                                .tokio_runtime_current_thread
                                .block_on(sql_dataset.scan_table(&sql_dataset.table_name))?;
                            VegaFusionDataset::DataFrame(df)
                        } else if inline_dataset.is_instance(df_dataset_type)? {
                            any_python_sources = true;

                            let df = Arc::new(PyDataFrame::new(inline_dataset.into_py(py))?);
                            VegaFusionDataset::DataFrame(df)
                        } else {
                            // Assume PyArrow Table
                            // We convert to ipc bytes for two reasons:
                            // - It allows VegaFusionDataset to compute an accurate hash of the table
                            // - It works around https://github.com/hex-inc/vegafusion/issues/268
                            let table = VegaFusionTable::from_pyarrow(py, inline_dataset)?;
                            VegaFusionDataset::from_table_ipc_bytes(&table.to_ipc_bytes()?)?
                        };

                        Ok((name.to_string(), dataset))
                    })
                    .collect::<PyResult<HashMap<_, _>>>()?;
                Ok((imported_datasets, any_python_sources))
            })
        } else {
            Ok((Default::default(), false))
        }
    }
}

#[pymethods]
impl PyVegaFusionRuntime {
    #[new]
    pub fn new(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
        connection: Option<PyObject>,
    ) -> PyResult<Self> {
        initialize_logging();

        let (conn, mut tokio_runtime_builder) = if let Some(pyconnection) = connection {
            // Use Python connection and single-threaded tokio runtime (this avoids deadlocking the Python interpreter)
            let conn = Arc::new(PySqlConnection::new(pyconnection)?) as Arc<dyn Connection>;
            (conn, tokio::runtime::Builder::new_current_thread())
        } else {
            // Use DataFusion connection and multi-threaded tokio runtime
            let conn = Arc::new(DataFusionConnection::default()) as Arc<dyn Connection>;
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads) = worker_threads {
                builder.worker_threads(worker_threads.max(1) as usize);
            }
            (conn, builder)
        };

        // Build the tokio runtime
        let tokio_runtime_connection = tokio_runtime_builder
            .enable_all()
            .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
            .build()
            .external("Failed to create Tokio thread pool")?;

        // Create current thread runtime
        let tokio_runtime_current_thread = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
            .build()
            .external("Failed to create Tokio thread pool")?;

        Ok(Self {
            runtime: VegaFusionRuntime::new(conn, max_capacity, memory_limit),
            tokio_runtime_connection: Arc::new(tokio_runtime_connection),
            tokio_runtime_current_thread: Arc::new(tokio_runtime_current_thread),
        })
    }

    pub fn process_request_bytes(&self, request_bytes: &PyBytes) -> PyResult<PyObject> {
        let response_bytes = self
            .tokio_runtime_connection
            .block_on(self.runtime.query_request_bytes(request_bytes.as_bytes()))?;
        Python::with_gil(|py| Ok(PyBytes::new(py, &response_bytes).into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform_spec(
        &self,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&PyDict>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let (inline_datasets, any_py_sources) = self.process_inline_datasets(inline_datasets)?;

        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(false);

        // Build keep_variables
        let mut keep_variables: Vec<ScopedVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push((Variable::new_signal(&name), scope))
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push((Variable::new_data(&name), scope))
        }

        // Get runtime based on whether there were any Python data sources
        // (in which case we need to use the current thread tokio runtime to avoid deadlocking)
        let rt = if any_py_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        let (spec, warnings) = rt.block_on(self.runtime.pre_transform_spec(
            &spec,
            &local_tz,
            &default_input_tz,
            row_limit,
            preserve_interactivity,
            inline_datasets,
            keep_variables,
        ))?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(PreTransformSpecWarningSpec::from)
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_spec = pythonize::pythonize(py, &spec)?;
            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_spec, py_warnings))
        })
    }

    pub fn pre_transform_datasets(
        &self,
        spec: PyObject,
        variables: Vec<(String, Vec<u32>)>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<(PyObject, PyObject)> {
        let (inline_datasets, any_py_sources) = self.process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;

        // Build variables
        let variables: Vec<ScopedVariable> = variables
            .iter()
            .map(|input_var| {
                let var = Variable::new_data(&input_var.0);
                let scope = input_var.1.clone();
                (var, scope)
            })
            .collect();

        // Get runtime based on whether there were any Python data sources
        // (in which case we need to use the current thread tokio runtime to avoid deadlocking)
        let rt = if any_py_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        let (values, warnings) = rt.block_on(self.runtime.pre_transform_values(
            &spec,
            &variables,
            &local_tz,
            &default_input_tz,
            row_limit,
            inline_datasets,
        ))?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(|warning| match warning.warning_type.as_ref().unwrap() {
                ValueWarningType::Planner(planner_warning) => PreTransformSpecWarningSpec {
                    typ: "Planner".to_string(),
                    message: planner_warning.message.clone(),
                },
                ValueWarningType::RowLimit(_) => PreTransformSpecWarningSpec {
                    typ: "RowLimitExceeded".to_string(),
                    message: "Some datasets in resulting Vega specification have been truncated to the provided row limit".to_string()
                }
            })
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_response_list = PyList::empty(py);
            for value in values {
                let pytable: PyObject = if let TaskValue::Table(table) = value {
                    table.to_pyarrow(py)?
                } else {
                    return Err(PyErr::from(VegaFusionError::internal(
                        "Unexpected value type",
                    )));
                };
                py_response_list.append(pytable)?;
            }

            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_response_list.into(), py_warnings))
        })
    }

    pub fn pre_transform_extract(
        &self,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<(PyObject, Vec<PyObject>, PyObject)> {
        let (inline_datasets, any_py_sources) = self.process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(true);

        // Get runtime based on whether there were any Python data sources
        // (in which case we need to use the current thread tokio runtime to avoid deadlocking)
        let rt = if any_py_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        let (tx_spec, datasets, warnings) = rt.block_on(self.runtime.pre_transform_extract(
            &spec,
            &local_tz,
            &default_input_tz,
            preserve_interactivity,
            inline_datasets,
        ))?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(|warning| match warning.warning_type.as_ref().unwrap() {
                ExtractWarningType::Planner(planner_warning) => PreTransformSpecWarningSpec {
                    typ: "Planner".to_string(),
                    message: planner_warning.message.clone(),
                },
            })
            .collect();

        Python::with_gil(|py| {
            let tx_spec = pythonize::pythonize(py, &tx_spec)?;

            let datasets = datasets
                .into_iter()
                .map(|(name, scope, table)| {
                    let name = name.into_py(py);
                    let scope = scope.into_py(py);
                    let table = table.to_pyarrow(py)?;
                    let dataset: PyObject = PyTuple::new(py, &[name, scope, table]).into_py(py);
                    Ok(dataset)
                })
                .collect::<PyResult<Vec<_>>>()?;

            let warnings = pythonize::pythonize(py, &warnings)?;

            Ok((tx_spec, datasets, warnings))
        })
    }

    pub fn patch_pre_transformed_spec(
        &self,
        spec1: PyObject,
        pre_transformed_spec1: PyObject,
        spec2: PyObject,
    ) -> PyResult<Option<PyObject>> {
        let spec1 = parse_json_spec(spec1)?;
        let pre_transformed_spec1 = parse_json_spec(pre_transformed_spec1)?;
        let spec2 = parse_json_spec(spec2)?;
        Python::with_gil(|py| {
            match patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)? {
                None => Ok(None),
                Some(pre_transformed_spec2) => {
                    Ok(Some(pythonize::pythonize(py, &pre_transformed_spec2)?))
                }
            }
        })
    }

    pub fn clear_cache(&self) {
        self.tokio_runtime_connection
            .block_on(self.runtime.clear_cache());
    }

    pub fn size(&self) -> usize {
        self.runtime.cache.size()
    }

    pub fn total_memory(&self) -> usize {
        self.runtime.cache.total_memory()
    }

    pub fn protected_memory(&self) -> usize {
        self.runtime.cache.protected_memory()
    }

    pub fn probationary_memory(&self) -> usize {
        self.runtime.cache.probationary_memory()
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn vegafusion_embed(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyVegaFusionRuntime>()?;
    m.add_class::<PySqlConnection>()?;
    Ok(())
}

/// Helper function to parse an input Python string or dict as a ChartSpec
fn parse_json_spec(chart_spec: PyObject) -> PyResult<ChartSpec> {
    Python::with_gil(|py| -> PyResult<ChartSpec> {
        if let Ok(chart_spec) = chart_spec.extract::<&str>(py) {
            match serde_json::from_str::<ChartSpec>(chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Failed to parse chart_spec string as Vega: {err}"
                ))),
            }
        } else if let Ok(chart_spec) = chart_spec.downcast::<PyDict>(py) {
            match depythonize(chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Failed to parse chart_spec dict as Vega: {err}"
                ))),
            }
        } else {
            Err(PyValueError::new_err("chart_spec must be a string or dict"))
        }
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
