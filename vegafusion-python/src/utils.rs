use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use pyo3::exceptions::PyValueError as PyValErr;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_arrow::PySchema;
use pythonize::depythonize;
use std::borrow::Cow;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlanBuilder;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::spec::chart::ChartSpec;

/// Convert optional inline datasets provided from Python into VegaFusion datasets.
pub fn process_inline_datasets(
    inline_datasets: Option<&Bound<PyDict>>,
) -> PyResult<HashMap<String, VegaFusionDataset>> {
    if let Some(inline_datasets) = inline_datasets {
        Python::with_gil(|py| -> PyResult<_> {
            let imported_datasets = inline_datasets
                .iter()
                .map(|(name, inline_dataset)| {
                    let inline_dataset = inline_dataset;
                    let dataset = if inline_dataset.hasattr("__arrow_c_stream__")? {
                        // Import via Arrow PyCapsule Interface
                        let (table, hash) =
                            VegaFusionTable::from_pyarrow_with_hash(py, &inline_dataset)?;
                        VegaFusionDataset::from_table(table, Some(hash))?
                    } else if let Ok(pyschema) = inline_dataset.extract::<PySchema>() {
                        // Handle PyArrow Schema as VegaFusionDataset::Plan
                        let schema = pyschema.into_inner();

                        // Build an empty table provider with the given schema and scan it
                        let provider = Arc::new(EmptyTable::new(schema.clone()));
                        let table_source = provider_as_source(provider);
                        let logical_plan =
                            LogicalPlanBuilder::scan(&name.to_string(), table_source, None)
                                .map_err(|e| {
                                    PyValErr::new_err(format!(
                                        "Failed to build logical plan from schema: {}",
                                        e
                                    ))
                                })?
                                .build()
                                .map_err(|e| {
                                    PyValErr::new_err(format!(
                                        "Failed to finalize logical plan from schema: {}",
                                        e
                                    ))
                                })?;

                        VegaFusionDataset::from_plan(logical_plan)
                    } else {
                        // Assume PyArrow Table
                        // We convert to ipc bytes for two reasons:
                        // - It allows VegaFusionDataset to compute an accurate hash of the table
                        // - It works around https://github.com/hex-inc/vegafusion/issues/268
                        let table = VegaFusionTable::from_pyarrow(py, &inline_dataset)?;
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

/// Parse a Python string or dict into a ChartSpec
pub fn parse_json_spec(chart_spec: PyObject) -> PyResult<ChartSpec> {
    Python::with_gil(|py| -> PyResult<ChartSpec> {
        if let Ok(chart_spec) = chart_spec.extract::<Cow<str>>(py) {
            match serde_json::from_str::<ChartSpec>(&chart_spec) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValErr::new_err(format!(
                    "Failed to parse chart_spec string as Vega: {err}"
                ))),
            }
        } else if let Ok(chart_spec) = chart_spec.downcast_bound::<PyAny>(py) {
            match depythonize::<ChartSpec>(&chart_spec.clone()) {
                Ok(chart_spec) => Ok(chart_spec),
                Err(err) => Err(PyValErr::new_err(format!(
                    "Failed to parse chart_spec dict as Vega: {err}"
                ))),
            }
        } else {
            Err(PyValErr::new_err("chart_spec must be a string or dict"))
        }
    })
}
