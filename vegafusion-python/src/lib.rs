mod chart_state;
mod plan_resolver;
mod unparse;
mod utils;
use lazy_static::lazy_static;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::str::FromStr;
use std::sync::{Arc, Once};
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Uri};

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
use serde_json::json;
use vegafusion_core::planning::plan::{PlannerConfig, PreTransformSpecWarningSpec, SpecPlan};
use vegafusion_core::planning::projection_pushdown::get_column_usage as rs_get_column_usage;
use vegafusion_core::planning::watch::WatchPlan;

use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::MaterializedTaskValue;
use vegafusion_runtime::tokio_runtime::TOKIO_THREAD_STACK_SIZE;

use vegafusion_core::runtime::{PlanResolver, VegaFusionRuntimeTrait};
use vegafusion_runtime::task_graph::cache::VegaFusionCache;

use crate::chart_state::PyChartState;
use crate::utils::{parse_json_spec, process_inline_datasets};

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
struct PyVegaFusionRuntime {
    runtime: Arc<dyn VegaFusionRuntimeTrait>,
    tokio_runtime: Arc<Runtime>,
}

impl PyVegaFusionRuntime {
    fn build_with_resolvers(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
        resolvers: Vec<Arc<dyn PlanResolver>>,
        use_current_thread: bool,
    ) -> PyResult<Self> {
        initialize_logging();

        let tokio_runtime_connection = if use_current_thread {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
                .build()
                .external("Failed to create Tokio current-thread runtime")?
        } else {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads) = worker_threads {
                builder.worker_threads(worker_threads.max(1) as usize);
            }
            builder
                .enable_all()
                .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
                .build()
                .external("Failed to create Tokio thread pool")?
        };

        Ok(Self {
            runtime: Arc::new(VegaFusionRuntime::new(
                Some(VegaFusionCache::new(max_capacity, memory_limit)),
                resolvers,
            )),
            tokio_runtime: Arc::new(tokio_runtime_connection),
        })
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
        Self::build_with_resolvers(
            max_capacity,
            memory_limit,
            worker_threads,
            Vec::new(),
            false,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (py_resolvers, max_capacity=None, memory_limit=None, worker_threads=None))]
    pub fn new_with_resolvers(
        py_resolvers: Vec<Py<PyAny>>,
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
    ) -> PyResult<Self> {
        let py_resolvers: Vec<crate::plan_resolver::PyPlanResolver> = py_resolvers
            .into_iter()
            .map(crate::plan_resolver::PyPlanResolver::new)
            .collect();

        // Use current-thread runtime if any resolver requires thread affinity
        let use_current_thread = py_resolvers.iter().any(|r| !r.thread_safe());

        let resolvers: Vec<Arc<dyn PlanResolver>> = py_resolvers
            .into_iter()
            .map(|r| Arc::new(r) as Arc<dyn PlanResolver>)
            .collect();
        Self::build_with_resolvers(
            max_capacity,
            memory_limit,
            worker_threads,
            resolvers,
            use_current_thread,
        )
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
        spec: Py<PyAny>,
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

        let inline_datasets = process_inline_datasets(inline_datasets)?;

        py.detach(|| {
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
        spec: Py<PyAny>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        preserve_interactivity: Option<bool>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;

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

        let (spec, warnings) = py.detach(|| {
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

        Python::attach(|py| -> PyResult<(Py<PyAny>, Py<PyAny>)> {
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
        spec: Py<PyAny>,
        variables: Vec<(String, Vec<u32>)>,
        local_tz: String,
        default_input_tz: Option<String>,
        row_limit: Option<u32>,
        inline_datasets: Option<&Bound<PyDict>>,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;
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

        let (values, warnings) = py.detach(|| {
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

        Python::attach(|py| -> PyResult<(Py<PyAny>, Py<PyAny>)> {
            let py_response_list = PyList::empty(py);
            for value in values {
                let pytable: Py<PyAny> = if let MaterializedTaskValue::Table(table) = value {
                    table.to_pyo3_arrow()?.into_pyarrow(py)?.into()
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
        spec: Py<PyAny>,
        local_tz: String,
        default_input_tz: Option<String>,
        preserve_interactivity: Option<bool>,
        extract_threshold: Option<usize>,
        extracted_format: Option<String>,
        inline_datasets: Option<&Bound<PyDict>>,
        keep_signals: Option<Vec<(String, Vec<u32>)>>,
        keep_datasets: Option<Vec<(String, Vec<u32>)>>,
    ) -> PyResult<(Py<PyAny>, Vec<Py<PyAny>>, Py<PyAny>)> {
        let inline_datasets = process_inline_datasets(inline_datasets)?;
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

        let (tx_spec, datasets, warnings) = py.detach(|| {
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

        Python::attach(|py| {
            let tx_spec = pythonize::pythonize(py, &tx_spec)?;

            let datasets = datasets
                .into_iter()
                .map(|tbl| {
                    let name: Py<PyAny> = tbl.name.into_pyobject(py)?.into();
                    let scope: Py<PyAny> = tbl.scope.into_pyobject(py)?.into();
                    let table = match extracted_format.as_str() {
                        "arro3" => {
                            let pytable = tbl.table.to_pyo3_arrow()?;
                            pytable.into_pyarrow(py)?.into()
                        }
                        "pyarrow" => tbl.table.to_pyo3_arrow()?.into_pyarrow(py)?.into(),
                        "arrow-ipc" => {
                            PyBytes::new(py, tbl.table.to_ipc_bytes()?.as_slice()).into()
                        }
                        "arrow-ipc-base64" => tbl.table.to_ipc_base64()?.into_pyobject(py)?.into(),
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid extracted_format: {}",
                                extracted_format
                            )))
                        }
                    };

                    let dataset: Py<PyAny> = PyTuple::new(py, &[name, scope, table])?.into();
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
pub fn get_column_usage(py: Python, spec: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let spec = parse_json_spec(spec)?;
    let usage = rs_get_column_usage(&spec)?;
    Ok(pythonize::pythonize(py, &usage)?.into())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (spec, preserve_interactivity=None, keep_signals=None, keep_datasets=None))]
pub fn build_pre_transform_spec_plan(
    spec: Py<PyAny>,
    preserve_interactivity: Option<bool>,
    keep_signals: Option<Vec<(String, Vec<u32>)>>,
    keep_datasets: Option<Vec<(String, Vec<u32>)>>,
) -> PyResult<Py<PyAny>> {
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

    Python::attach(|py| -> PyResult<Py<PyAny>> {
        let py_plan = pythonize::pythonize(py, &json_plan)?;
        Ok(py_plan.into())
    })
}

/// Build a LogicalPlanNode protobuf (as bytes) for an inline table scan.
///
/// Use this in `resolve_plan` implementations to replace a subtree that the
/// resolver has already executed with a leaf node referencing sidecar Arrow data.
///
/// Args:
///     name: Key that will match an entry in ResolvedPlan.datasets.
///     schema: Arrow schema of the sidecar table (arro3.core.Schema).
///
/// Returns:
///     bytes: Serialized LogicalPlanNode protobuf.
#[pyfunction]
#[pyo3(signature = (name, schema))]
pub fn inline_table_scan_node(name: String, schema: pyo3_arrow::PySchema) -> PyResult<Vec<u8>> {
    use datafusion::datasource::provider_as_source;
    use datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec;
    use vegafusion_common::datafusion_expr::LogicalPlanBuilder;
    use vegafusion_runtime::data::codec::VegaFusionCodec;
    use vegafusion_runtime::data::inline_table::InlineTableProvider;

    let arrow_schema = schema.into_inner();
    let provider = Arc::new(InlineTableProvider::new(arrow_schema, name.clone()));
    let table_source = provider_as_source(provider);

    let plan = LogicalPlanBuilder::scan(&name, table_source, None)
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to build scan plan: {e}"))
        })?
        .build()
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to build plan: {e}"))
        })?;

    let codec = VegaFusionCodec::new();
    let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Failed to serialize inline table plan: {e}"
        ))
    })?;

    Ok(bytes.to_vec())
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
    m.add_function(wrap_pyfunction!(inline_table_scan_node, m)?)?;
    m.add_function(wrap_pyfunction!(unparse::unparse_plan_to_sql, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
