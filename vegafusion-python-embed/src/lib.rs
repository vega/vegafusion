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
use vegafusion_runtime::task_graph::runtime::{ChartState as RsChartState, VegaFusionRuntime};

use crate::connection::{PySqlConnection, PySqlDataset};
use crate::dataframe::PyDataFrame;
use env_logger::{Builder, Target};
use pythonize::{depythonize, pythonize};
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::patch::patch_pre_transformed_spec;
use vegafusion_core::planning::plan::{PlannerConfig, PreTransformSpecWarningSpec, SpecPlan};
use vegafusion_core::planning::watch::{ExportUpdateJSON, WatchPlan};
use vegafusion_core::proto::gen::tasks::{TzConfig, Variable};
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
struct PyChartState {
    runtime: Arc<VegaFusionRuntime>,
    state: RsChartState,
    tokio_runtime: Arc<Runtime>,
}

impl PyChartState {
    pub fn try_new(
        runtime: Arc<VegaFusionRuntime>,
        tokio_runtime: Arc<Runtime>,
        spec: ChartSpec,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        tz_config: TzConfig,
        row_limit: Option<u32>,
    ) -> PyResult<Self> {
        let state = tokio_runtime.block_on(RsChartState::try_new(
            &runtime,
            spec,
            inline_datasets,
            tz_config,
            row_limit,
        ))?;
        Ok(Self {
            runtime,
            state,
            tokio_runtime,
        })
    }
}

#[pymethods]
impl PyChartState {
    /// Update chart state with updates from the client
    pub fn update(&self, py: Python, updates: Vec<PyObject>) -> PyResult<Vec<PyObject>> {
        let updates = updates
            .into_iter()
            .map(|el| Ok(depythonize::<ExportUpdateJSON>(el.as_ref(py))?))
            .collect::<PyResult<Vec<_>>>()?;

        let result_updates = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.state.update(&self.runtime, updates))
        })?;

        let a = result_updates
            .into_iter()
            .map(|el| Ok(pythonize(py, &el)?))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(a)
    }

    /// Get ChartState's initial input spec
    pub fn get_input_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_input_spec())?)
    }

    /// Get ChartState's server spec
    pub fn get_server_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_server_spec())?)
    }

    /// Get ChartState's client spec
    pub fn get_client_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_client_spec())?)
    }

    /// Get ChartState's initial transformed spec
    pub fn get_transformed_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_transformed_spec())?)
    }

    /// Get ChartState's watch plan
    pub fn get_watch_plan(&self, py: Python) -> PyResult<PyObject> {
        let watch_plan = WatchPlan::from(self.state.get_comm_plan().clone());
        Ok(pythonize(py, &watch_plan)?)
    }

    /// Get list of transform warnings
    pub fn get_warnings(&self, py: Python) -> PyResult<PyObject> {
        let warnings: Vec<_> = self
            .state
            .get_warnings()
            .iter()
            .map(PreTransformSpecWarningSpec::from)
            .collect();
        Ok(pythonize::pythonize(py, &warnings)?)
    }
}

#[pyclass]
struct PyVegaFusionRuntime {
    runtime: Arc<VegaFusionRuntime>,
    tokio_runtime_connection: Arc<Runtime>,
    tokio_runtime_current_thread: Arc<Runtime>,
}

impl PyVegaFusionRuntime {
    fn process_inline_datasets(
        &self,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<(HashMap<String, VegaFusionDataset>, bool)> {
        let mut any_main_thread = false;
        if let Some(inline_datasets) = inline_datasets {
            Python::with_gil(|py| -> PyResult<_> {
                let vegafusion_dataset_module = PyModule::import(py, "vegafusion.dataset")?;
                let sql_dataset_type = vegafusion_dataset_module.getattr("SqlDataset")?;
                let df_dataset_type = vegafusion_dataset_module.getattr("DataFrameDataset")?;

                let vegafusion_datasource_module = PyModule::import(py, "vegafusion.datasource")?;
                let datasource_type = vegafusion_datasource_module.getattr("Datasource")?;

                let imported_datasets = inline_datasets
                    .iter()
                    .map(|(name, inline_dataset)| {
                        let dataset = if inline_dataset.is_instance(sql_dataset_type)? {
                            let main_thread = inline_dataset
                                .call_method0("main_thread")?
                                .extract::<bool>()?;
                            any_main_thread = any_main_thread || main_thread;
                            let sql_dataset = PySqlDataset::new(inline_dataset.into_py(py))?;
                            let rt = if main_thread {
                                &self.tokio_runtime_current_thread
                            } else {
                                &self.tokio_runtime_connection
                            };
                            let df = py.allow_threads(|| {
                                rt.block_on(sql_dataset.scan_table(&sql_dataset.table_name))
                            })?;
                            VegaFusionDataset::DataFrame(df)
                        } else if inline_dataset.is_instance(df_dataset_type)? {
                            let main_thread = inline_dataset
                                .call_method0("main_thread")?
                                .extract::<bool>()?;
                            any_main_thread = any_main_thread || main_thread;

                            let df = Arc::new(PyDataFrame::new(inline_dataset.into_py(py))?);
                            VegaFusionDataset::DataFrame(df)
                        } else if inline_dataset.is_instance(datasource_type)? {
                            let df = self.tokio_runtime_connection.block_on(
                                self.runtime
                                    .conn
                                    .scan_py_datasource(inline_dataset.to_object(py)),
                            )?;
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
                Ok((imported_datasets, any_main_thread))
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
            runtime: Arc::new(VegaFusionRuntime::new(conn, max_capacity, memory_limit)),
            tokio_runtime_connection: Arc::new(tokio_runtime_connection),
            tokio_runtime_current_thread: Arc::new(tokio_runtime_current_thread),
        })
    }

    pub fn new_chart_state(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<PyChartState> {
        let spec = parse_json_spec(spec)?;
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };

        let (inline_datasets, any_main_thread_sources) =
            self.process_inline_datasets(inline_datasets)?;

        // Get runtime based on whether there were any Python data sources that require running
        // on the main thread. In this case we need to use the current thread tokio runtime
        let tokio_runtime = if any_main_thread_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        py.allow_threads(|| {
            PyChartState::try_new(
                self.runtime.clone(),
                tokio_runtime.clone(),
                spec,
                inline_datasets,
                tz_config,
                row_limit,
            )
        })
    }

    pub fn process_request_bytes(&self, py: Python, request_bytes: &PyBytes) -> PyResult<PyObject> {
        let request_bytes = request_bytes.as_bytes();
        let response_bytes = py.allow_threads(|| {
            self.tokio_runtime_connection
                .block_on(self.runtime.query_request_bytes(request_bytes))
        })?;
        Ok(PyBytes::new(py, &response_bytes).into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_pre_transform_spec_plan(
        &self,
        spec: PyObject,
        preserve_interactivity: Option<bool>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<PyObject> {
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

        let plan = SpecPlan::try_new(
            &spec,
            &PlannerConfig::pre_transformed_spec_config(preserve_interactivity, keep_variables),
        )?;
        let watch_plan = WatchPlan::from(plan.comm_plan);

        let json_plan = json!({
            "server_spec": plan.server_spec,
            "client_spec": plan.client_spec,
            "comm_plan": watch_plan,
            "warnings": plan.warnings,
        });

        Python::with_gil(|py| -> PyResult<PyObject> {
            let py_plan = pythonize::pythonize(py, &json_plan)?;
            Ok(py_plan)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform_spec(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&PyDict>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let (inline_datasets, any_main_thread_sources) =
            self.process_inline_datasets(inline_datasets)?;

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

        // Get runtime based on whether there were any Python data sources that require running
        // on the main thread. In this case we need to use the current thread tokio runtime
        let rt = if any_main_thread_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        let (spec, warnings) = py.allow_threads(|| {
            rt.block_on(self.runtime.pre_transform_spec(
                &spec,
                &local_tz,
                &default_input_tz,
                row_limit,
                preserve_interactivity,
                inline_datasets,
                keep_variables,
            ))
        })?;

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
        py: Python,
        spec: PyObject,
        variables: Vec<(String, Vec<u32>)>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&PyDict>,
    ) -> PyResult<(PyObject, PyObject)> {
        let (inline_datasets, any_main_thread_sources) =
            self.process_inline_datasets(inline_datasets)?;
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

        // Get runtime based on whether there were any Python data sources that require running
        // on the main thread. In this case we need to use the current thread tokio runtime
        let rt = if any_main_thread_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        let (values, warnings) = py.allow_threads(|| {
            rt.block_on(self.runtime.pre_transform_values(
                &spec,
                &variables,
                &local_tz,
                &default_input_tz,
                row_limit,
                inline_datasets,
            ))
        })?;

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

    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform_extract(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        extract_threshold: Option<usize>,
        extracted_format: Option<String>,
        inline_datasets: Option<&PyDict>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, Vec<PyObject>, PyObject)> {
        let (inline_datasets, any_main_thread_sources) =
            self.process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(true);
        let extract_threshold = extract_threshold.unwrap_or(20);
        let extracted_format = extracted_format.unwrap_or_else(|| "pyarrow".to_string());

        // Get runtime based on whether there were any Python data sources that require running
        // on the main thread. In this case we need to use the current thread tokio runtime
        let rt = if any_main_thread_sources {
            &self.tokio_runtime_current_thread
        } else {
            &self.tokio_runtime_connection
        };

        // Build keep_variables
        let mut keep_variables: Vec<ScopedVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push((Variable::new_signal(&name), scope))
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push((Variable::new_data(&name), scope))
        }

        let (tx_spec, datasets, warnings) = py.allow_threads(|| {
            rt.block_on(self.runtime.pre_transform_extract(
                &spec,
                &local_tz,
                &default_input_tz,
                preserve_interactivity,
                extract_threshold,
                inline_datasets,
                keep_variables,
            ))
        })?;

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
                    let table = match extracted_format.as_str() {
                        "pyarrow" => table.to_pyarrow(py)?,
                        "arrow-ipc" => {
                            PyBytes::new(py, table.to_ipc_bytes()?.as_slice()).to_object(py)
                        }
                        "arrow-ipc-base64" => table.to_ipc_base64()?.into_py(py),
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid extracted_format: {}",
                                extracted_format
                            )))
                        }
                    };

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

    pub fn clear_cache(&self, py: Python) {
        py.allow_threads(|| {
            self.tokio_runtime_connection
                .block_on(self.runtime.clear_cache())
        });
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
    m.add_class::<PyChartState>()?;
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
