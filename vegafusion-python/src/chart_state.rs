use std::sync::Arc;

use pyo3::prelude::*;
use pythonize::{depythonize, pythonize};
use std::collections::HashMap;
use tokio::runtime::Runtime;
use vegafusion_core::planning::watch::ExportUpdateJSON;
use vegafusion_core::{
    chart_state::{ChartState as RsChartState, ChartStateOpts},
    data::dataset::VegaFusionDataset,
    planning::{plan::PreTransformSpecWarningSpec, watch::WatchPlan},
    proto::gen::tasks::TzConfig,
    runtime::VegaFusionRuntimeTrait,
    spec::chart::ChartSpec,
};

#[pyclass]
pub struct PyChartState {
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
            .map(|el| Ok(depythonize::<ExportUpdateJSON>(&el.bind(py))?))
            .collect::<PyResult<Vec<_>>>()?;

        let result_updates = py.allow_threads(|| {
            self.tokio_runtime
                .block_on(self.state.update(self.runtime.as_ref(), updates))
        })?;

        let py_updates = result_updates
            .into_iter()
            .map(|el| Ok(pythonize(py, &el)?.into()))
            .collect::<PyResult<Vec<PyObject>>>()?;
        Ok(py_updates)
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
        Ok(pythonize(py, &warnings)?.into())
    }
}
