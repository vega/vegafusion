use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::Runtime;
use vegafusion_core::error::ToExternalError;
use vegafusion_core::proto::gen::tasks::TaskGraph;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

#[pyclass]
struct PyTaskGraphRuntime {
    runtime: TaskGraphRuntime,
    tokio_runtime: Runtime,
}

#[pymethods]
impl PyTaskGraphRuntime {
    #[new]
    pub fn new(max_capacity: i32, worker_threads: Option<i32>) -> PyResult<Self> {
        let mut tokio_runtime_builder = tokio::runtime::Builder::new_multi_thread();
        tokio_runtime_builder.enable_all();

        if let Some(worker_threads) = worker_threads {
            tokio_runtime_builder.worker_threads(worker_threads.max(1) as usize);
        }

        // Build tokio runtime
        let tokio_runtime = tokio_runtime_builder
            .build()
            .external("Failed to create Tokio thread pool")?;

        Ok(Self {
            runtime: TaskGraphRuntime::new(max_capacity as usize),
            tokio_runtime,
        })
    }

    pub fn process_request_bytes(&self, request_bytes: Vec<u8>) -> PyResult<PyObject> {
        let response_bytes = self
            .tokio_runtime
            .block_on(self.runtime.process_request_bytes(request_bytes))?;
        Python::with_gil(|py| Ok(PyBytes::new(py, &response_bytes).into()))
    }

    pub fn clear_cache(&self) {
        self.tokio_runtime.block_on(self.runtime.clear_cache());
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn vegafusion(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyTaskGraphRuntime>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
