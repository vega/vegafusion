/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::Runtime;
use vegafusion_core::error::ToExternalError;

use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

#[pyclass]
struct PyTaskGraphRuntime {
    runtime: TaskGraphRuntime,
    tokio_runtime: Runtime,
}

#[pymethods]
impl PyTaskGraphRuntime {
    #[new]
    pub fn new(
        max_capacity: Option<usize>,
        memory_limit: Option<usize>,
        worker_threads: Option<i32>,
    ) -> PyResult<Self> {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
