use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use std::collections::HashMap;
use std::sync::Once;
use tokio::runtime::Runtime;
use vegafusion_core::error::{ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::pretransform::pre_transform_spec_warning::WarningType;
use vegafusion_core::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValueWarningType;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

use env_logger::{Builder, Target};
use pythonize::depythonize;
use serde::{Deserialize, Serialize};
use vegafusion_core::proto::gen::tasks::Variable;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_rt_datafusion::data::dataset::VegaFusionDataset;
use vegafusion_rt_datafusion::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

static INIT: Once = Once::new();

pub fn initialize_logging() {
    INIT.call_once(|| {
        // Init logger to write to stdout
        let mut builder = Builder::from_default_env();
        builder.target(Target::Stdout);
        builder.init();
    });
}

#[derive(Clone, Serialize, Deserialize)]
struct PreTransformSpecWarning {
    #[serde(rename = "type")]
    pub typ: String,
    pub message: String,
}

#[pyclass]
struct PyTaskGraphRuntime {
    runtime: TaskGraphRuntime,
    tokio_runtime: Runtime,
}

fn deserialize_inline_datasets(
    inline_datasets: &PyDict,
) -> PyResult<HashMap<String, VegaFusionDataset>> {
    inline_datasets
        .iter()
        .map(|(name, table_bytes)| {
            let name = name.cast_as::<PyString>()?;
            let ipc_bytes = table_bytes.cast_as::<PyBytes>()?;
            let ipc_bytes = ipc_bytes.as_bytes();
            let dataset = VegaFusionDataset::from_table_ipc_bytes(ipc_bytes)?;
            Ok((name.to_string(), dataset))
        })
        .collect::<PyResult<HashMap<_, _>>>()
}

#[pymethods]
impl PyTaskGraphRuntime {
    #[new]
    pub fn new(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
    ) -> PyResult<Self> {
        initialize_logging();

        let mut tokio_runtime_builder = tokio::runtime::Builder::new_multi_thread();
        tokio_runtime_builder.enable_all();

        if let Some(worker_threads) = worker_threads {
            tokio_runtime_builder.worker_threads(worker_threads.max(1) as usize);
        }

        tokio_runtime_builder.thread_stack_size(TOKIO_THREAD_STACK_SIZE);

        // Build tokio runtime
        let tokio_runtime = tokio_runtime_builder
            .build()
            .external("Failed to create Tokio thread pool")?;

        Ok(Self {
            runtime: TaskGraphRuntime::new(max_capacity, memory_limit),
            tokio_runtime,
        })
    }

    pub fn process_request_bytes(&self, request_bytes: &PyBytes) -> PyResult<PyObject> {
        let response_bytes = self
            .tokio_runtime
            .block_on(self.runtime.query_request_bytes(request_bytes.as_bytes()))?;
        Python::with_gil(|py| Ok(PyBytes::new(py, &response_bytes).into()))
    }

    pub fn pre_transform_spec(
        &self,
        spec: PyObject,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: &PyDict,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = deserialize_inline_datasets(inline_datasets)?;
        let spec = parse_json_spec(spec)?;
        let preserve_interactivity = preserve_interactivity.unwrap_or(false);

        let (spec, warnings) = self
            .tokio_runtime
            .block_on(self.runtime.pre_transform_spec(
                &spec,
                &local_tz,
                &default_input_tz,
                row_limit,
                preserve_interactivity,
                inline_datasets,
            ))?;

        let warnings: Vec<_> = warnings.iter().map(|warning| {
            match warning.warning_type.as_ref().unwrap() {
                WarningType::RowLimit(_) => {
                    PreTransformSpecWarning {
                        typ: "RowLimitExceeded".to_string(),
                        message: "Some datasets in resulting Vega specification have been truncated to the provided row limit".to_string()
                    }
                }
                WarningType::BrokenInteractivity(_) => {
                    PreTransformSpecWarning {
                        typ: "BrokenInteractivity".to_string(),
                        message: "Some interactive features may have been broken in the resulting Vega specification".to_string()
                    }
                }
                WarningType::Unsupported(_) => {
                    PreTransformSpecWarning {
                        typ: "Unsupported".to_string(),
                        message: "Unable to pre-transform any datasets in the Vega specification".to_string()
                    }
                }
                WarningType::Planner(warning) => {
                    PreTransformSpecWarning {
                        typ: "Planner".to_string(),
                        message: warning.message.clone()
                    }
                }
            }
        }).collect();

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
        inline_datasets: &PyDict,
    ) -> PyResult<(PyObject, PyObject)> {
        let inline_datasets = deserialize_inline_datasets(inline_datasets)?;
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

        let (values, warnings) = self
            .tokio_runtime
            .block_on(self.runtime.pre_transform_values(
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
                ValueWarningType::Planner(planner_warning) => PreTransformSpecWarning {
                    typ: "Planner".to_string(),
                    message: planner_warning.message.clone(),
                },
                ValueWarningType::RowLimit(_) => PreTransformSpecWarning {
                    typ: "RowLimitExceeded".to_string(),
                    message: "Some datasets in resulting Vega specification have been truncated to the provided row limit".to_string()
                }
            })
            .collect();

        Python::with_gil(|py| -> PyResult<(PyObject, PyObject)> {
            let py_response_list = PyList::empty(py);
            for value in values {
                let bytes: PyObject = if let TaskValue::Table(table) = value {
                    PyBytes::new(py, table.to_ipc_bytes()?.as_slice()).into()
                } else {
                    return Err(PyErr::from(VegaFusionError::internal(
                        "Unexpected value type",
                    )));
                };
                py_response_list.append(bytes)?;
            }

            let py_warnings = pythonize::pythonize(py, &warnings)?;
            Ok((py_response_list.into(), py_warnings))
        })
    }

    pub fn clear_cache(&self) {
        self.tokio_runtime.block_on(self.runtime.clear_cache());
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
    m.add_class::<PyTaskGraphRuntime>()?;
    Ok(())
}

/// Helper function to parse an input Python string or dict as a ChartSpec
fn parse_json_spec(chart_spec: PyObject) -> PyResult<ChartSpec> {
    Python::with_gil(|py| -> PyResult<ChartSpec> {
        if let Ok(chart_spec) = chart_spec.extract::<&str>(py) {
            match serde_json::from_str::<ChartSpec>(chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Failed to parse chart_spec string as Vega: {}",
                    err
                ))),
            }
        } else if let Ok(chart_spec) = chart_spec.cast_as::<PyDict>(py) {
            match depythonize(chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Failed to parse chart_spec dict as Vega: {}",
                    err
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
