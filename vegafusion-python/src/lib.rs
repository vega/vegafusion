use lazy_static::lazy_static;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Once};
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Uri};
use vegafusion_core::chart_state::{ChartState as RsChartState, ChartStateOpts};
use vegafusion_core::error::{ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::pretransform::pre_transform_extract_warning::WarningType as ExtractWarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValueWarningType;
use vegafusion_core::proto::gen::pretransform::{
    PreTransformExtractOpts, PreTransformSpecOpts, PreTransformValuesOpts, PreTransformVariable,
};
use vegafusion_core::proto::gen::tasks::{TzConfig, Variable};
use vegafusion_runtime::task_graph::GrpcVegaFusionRuntime;

use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

use env_logger::{Builder, Target};
use pythonize::{depythonize, pythonize};
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::planning::plan::{PlannerConfig, PreTransformSpecWarningSpec, SpecPlan};
use vegafusion_core::planning::projection_pushdown::get_column_usage as rs_get_column_usage;
use vegafusion_core::planning::watch::{ExportUpdateJSON, WatchPlan};

use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_runtime::task_graph::cache::VegaFusionCache;

static INIT: Once = Once::new();

pub fn initialize_logging() {
    INIT.call_once(|| {
        // Init logger to write to stdout
        let mut builder = Builder::from_default_env();
        builder.target(Target::Stdout);
        builder.init();
    });
}

lazy_static! {
    static ref SYSTEM_INSTANCE: sysinfo::System = sysinfo::System::new_all();
}

#[pyclass]
struct PyChartState {
    runtime: Arc<dyn VegaFusionRuntimeTrait>,
    state: RsChartState,
    tokio_runtime: Arc<Runtime>,
}

impl PyChartState {
    pub fn try_new(
        runtime: Arc<dyn VegaFusionRuntimeTrait>,
        tokio_runtime: Arc<Runtime>,
        spec: ChartSpec,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        tz_config: TzConfig,
        row_limit: Option<u32>,
    ) -> PyResult<Self> {
        let state = tokio_runtime.block_on(RsChartState::try_new(
            runtime.as_ref(),
            spec,
            inline_datasets,
            ChartStateOpts {
                tz_config,
                row_limit,
            },
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
            .map(|el| Ok(depythonize::<ExportUpdateJSON>(&el.bind(py).clone())?))
            .collect::<PyResult<Vec<_>>>()?;

        let result_updates = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.state.update(self.runtime.as_ref(), updates))
        })?;

        let a = result_updates
            .into_iter()
            .map(|el| Ok(pythonize(py, &el)?.into()))
            .collect::<PyResult<Vec<PyObject>>>()?;
        Ok(a)
    }

    /// Get ChartState's initial input spec
    pub fn get_input_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_input_spec())?.into())
    }

    /// Get ChartState's server spec
    pub fn get_server_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_server_spec())?.into())
    }

    /// Get ChartState's client spec
    pub fn get_client_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_client_spec())?.into())
    }

    /// Get ChartState's initial transformed spec
    pub fn get_transformed_spec(&self, py: Python) -> PyResult<PyObject> {
        Ok(pythonize(py, self.state.get_transformed_spec())?.into())
    }

    /// Get ChartState's watch plan
    pub fn get_comm_plan(&self, py: Python) -> PyResult<PyObject> {
        let comm_plan = WatchPlan::from(self.state.get_comm_plan().clone());
        Ok(pythonize(py, &comm_plan)?.into())
    }

    /// Get list of transform warnings
    pub fn get_warnings(&self, py: Python) -> PyResult<PyObject> {
        let warnings: Vec<_> = self
            .state
            .get_warnings()
            .iter()
            .map(PreTransformSpecWarningSpec::from)
            .collect();
        Ok(pythonize::pythonize(py, &warnings)?.into())
    }
}

#[pyclass]
struct PyVegaFusionRuntime {
    runtime: Arc<dyn VegaFusionRuntimeTrait>,
    tokio_runtime: Arc<Runtime>,
}

impl PyVegaFusionRuntime {
    fn process_inline_datasets(
        &self,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<HashMap<String, VegaFusionDataset>> {
        if let Some(inline_datasets) = inline_datasets {
            Python::with_gil(|py| -> PyResult<_> {
                let imported_datasets = inline_datasets
                    .iter()
                    .map(|(name, inline_dataset)| {
                        let inline_dataset = inline_dataset.to_object(py);
                        let inline_dataset = inline_dataset.bind(py);
                        let dataset = if inline_dataset.hasattr("__arrow_c_stream__")? {
                            // Import via Arrow PyCapsule Interface
                            let (table, hash) =
                                VegaFusionTable::from_pyarrow_with_hash(py, inline_dataset)?;
                            VegaFusionDataset::from_table(table, Some(hash))?
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
                Ok(imported_datasets)
            })
        } else {
            Ok(Default::default())
        }
    }
}

#[pymethods]
impl PyVegaFusionRuntime {
    #[staticmethod]
    #[pyo3(signature = (max_capacity=None, memory_limit=None, worker_threads=None))]
    pub fn new_embedded(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
    ) -> PyResult<Self> {
        initialize_logging();

        // Use DataFusion connection and multi-threaded tokio runtime
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = worker_threads {
            builder.worker_threads(worker_threads.max(1) as usize);
        }

        // Build the tokio runtime
        let tokio_runtime_connection = builder
            .enable_all()
            .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
            .build()
            .external("Failed to create Tokio thread pool")?;

        Ok(Self {
            runtime: Arc::new(VegaFusionRuntime::new(Some(VegaFusionCache::new(
                max_capacity,
                memory_limit,
            )))),
            tokio_runtime: Arc::new(tokio_runtime_connection),
        })
    }

    #[staticmethod]
    pub fn new_grpc(url: &str) -> PyResult<Self> {
        let tokio_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let runtime = tokio_runtime.block_on(async move {
            let innter_url = url;
            let uri =
                Uri::from_str(innter_url).map_err(|e| VegaFusionError::internal(e.to_string()))?;

            GrpcVegaFusionRuntime::try_new(Channel::builder(uri).connect().await.map_err(|e| {
                let msg = format!("Error connecting to gRPC server at {}: {}", innter_url, e);
                VegaFusionError::internal(msg)
            })?)
            .await
        })?;

        Ok(Self {
            runtime: Arc::new(runtime),
            tokio_runtime: tokio_runtime.clone(),
        })
    }

    #[pyo3(signature = (spec, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None))]
    pub fn new_chart_state(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<PyChartState> {
        let spec = parse_json_spec(spec)?;
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };

        let inline_datasets = self.process_inline_datasets(inline_datasets)?;

        py.allow_threads(|| {
            PyChartState::try_new(
                self.runtime.clone(),
                self.tokio_runtime.clone(),
                spec,
                inline_datasets,
                tz_config,
                row_limit,
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spec, local_tz, default_input_tz=None, row_limit=None, preserve_interactivity=None, inline_datasets=None, keep_signals=None, keep_datasets=None))]
    pub fn pre_transform_spec(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = self.process_inline_datasets(inline_datasets)?;

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

        let (spec, warnings) = py.allow_threads(|| {
            self.tokio_runtime.block_on(
                self.runtime.pre_transform_spec(
                    &spec,
                    &inline_datasets,
                    &PreTransformSpecOpts {
                        local_tz,
                        default_input_tz,
                        row_limit,
                        preserve_interactivity,
                        keep_variables: keep_variables
                            .into_iter()
                            .map(|v| PreTransformVariable {
                                variable: Some(v.0),
                                scope: v.1,
                            })
                            .collect(),
                    },
                ),
            )
        })?;

        let warnings: Vec<_> = warnings
            .iter()
            .map(PreTransformSpecWarningSpec::from)
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_spec = pythonize::pythonize(py, &spec)?;
            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_spec.into(), py_warnings.into()))
        })
    }

    #[pyo3(signature = (spec, variables, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform_datasets(
        &self,
        py: Python,
        spec: PyObject,
        variables: Vec<(String, Vec<u32>)>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = self.process_inline_datasets(inline_datasets)?;
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

        let (values, warnings) = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.runtime.pre_transform_values(
                    &spec,
                    &variables,
                    &inline_datasets,
                    &PreTransformValuesOpts {
                        local_tz,
                        default_input_tz,
                        row_limit,
                    },
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
            let py_response_list = PyList::empty_bound(py);
            for value in values {
                let pytable: PyObject = if let TaskValue::Table(table) = value {
                    table.to_pyo3_arrow()?.into_py(py)
                } else {
                    return Err(PyErr::from(VegaFusionError::internal(
                        "Unexpected value type",
                    )));
                };
                py_response_list.append(pytable)?;
            }

            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_response_list.into(), py_warnings.into()))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spec, local_tz, default_input_tz=None, preserve_interactivity=None, extract_threshold=None, extracted_format=None, inline_datasets=None, keep_signals=None, keep_datasets=None))]
    pub fn pre_transform_extract(
        &self,
        py: Python,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        extract_threshold: Option<usize>,
        extracted_format: Option<String>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(PyObject, Vec<PyObject>, PyObject)> {
        let inline_datasets = self.process_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(true);
        let extract_threshold = extract_threshold.unwrap_or(20);
        let extracted_format = extracted_format.unwrap_or_else(|| "pyarrow".to_string());

        // Build keep_variables
        let mut keep_variables: Vec<PreTransformVariable> = Vec::new();
        for (name, scope) in keep_signals.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_signal(&name)),
                scope,
            });
        }
        for (name, scope) in keep_datasets.unwrap_or_default() {
            keep_variables.push(PreTransformVariable {
                variable: Some(Variable::new_data(&name)),
                scope,
            });
        }

        let (tx_spec, datasets, warnings) = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.runtime.pre_transform_extract(
                    &spec,
                    &inline_datasets,
                    &PreTransformExtractOpts {
                        local_tz,
                        default_input_tz,
                        preserve_interactivity,
                        extract_threshold: extract_threshold as i32,
                        keep_variables,
                    },
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
                .map(|tbl| {
                    let name = tbl.name.into_py(py);
                    let scope = tbl.scope.into_py(py);
                    let table = match extracted_format.as_str() {
                        "arro3" => tbl.table.to_pyo3_arrow()?.into_py(py),
                        "pyarrow" => tbl.table.to_pyo3_arrow()?.to_pyarrow(py)?.into_py(py),
                        "arrow-ipc" => PyBytes::new_bound(py, tbl.table.to_ipc_bytes()?.as_slice())
                            .to_object(py),
                        "arrow-ipc-base64" => tbl.table.to_ipc_base64()?.into_py(py),
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid extracted_format: {}",
                                extracted_format
                            )))
                        }
                    };

                    let dataset: PyObject =
                        PyTuple::new_bound(py, &[name, scope, table]).into_py(py);
                    Ok(dataset)
                })
                .collect::<PyResult<Vec<_>>>()?;

            let warnings = pythonize::pythonize(py, &warnings)?;

            Ok((tx_spec.into(), datasets, warnings.into()))
        })
    }

    pub fn clear_cache(&self) -> PyResult<()> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            self.tokio_runtime.block_on(runtime.clear_cache());
            Ok(())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support clear_cache",
            ))
        }
    }

    pub fn size(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.size())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support size",
            ))
        }
    }

    pub fn total_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.total_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support total_memory",
            ))
        }
    }

    pub fn protected_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.protected_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support protected_memory",
            ))
        }
    }

    pub fn probationary_memory(&self) -> PyResult<usize> {
        if let Some(runtime) = self.runtime.as_any().downcast_ref::<VegaFusionRuntime>() {
            Ok(runtime.cache.probationary_memory())
        } else {
            Err(PyValueError::new_err(
                "Current Runtime does not support probationary_memory",
            ))
        }
    }
}

#[pyfunction]
#[pyo3(signature = ())]
pub fn get_virtual_memory() -> u64 {
    SYSTEM_INSTANCE.total_memory()
}

#[pyfunction]
#[pyo3(signature = ())]
pub fn get_cpu_count() -> u64 {
    SYSTEM_INSTANCE.cpus().len() as u64
}

#[pyfunction]
#[pyo3(signature = (spec))]
pub fn get_column_usage(py: Python, spec: PyObject) -> PyResult<PyObject> {
    let spec = parse_json_spec(spec)?;
    let usage = rs_get_column_usage(&spec)?;
    Ok(pythonize::pythonize(py, &usage)?.into())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (spec, preserve_interactivity=None, keep_signals=None, keep_datasets=None))]
pub fn build_pre_transform_spec_plan(
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
        Ok(py_plan.into())
    })
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _vegafusion(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyVegaFusionRuntime>()?;
    m.add_class::<PyChartState>()?;
    m.add_function(wrap_pyfunction!(get_column_usage, m)?)?;
    m.add_function(wrap_pyfunction!(build_pre_transform_spec_plan, m)?)?;
    m.add_function(wrap_pyfunction!(get_virtual_memory, m)?)?;
    m.add_function(wrap_pyfunction!(get_cpu_count, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

/// Helper function to parse an input Python string or dict as a ChartSpec
fn parse_json_spec(chart_spec: PyObject) -> PyResult<ChartSpec> {
    Python::with_gil(|py| -> PyResult<ChartSpec> {
        if let Ok(chart_spec) = chart_spec.extract::<Cow<str>>(py) {
            match serde_json::from_str::<ChartSpec>(&chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Failed to parse chart_spec string as Vega: {err}"
                ))),
            }
        } else if let Ok(chart_spec) = chart_spec.downcast_bound::<PyAny>(py) {
            match depythonize::<ChartSpec>(&chart_spec.clone()) {
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
