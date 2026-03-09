use std::collections::HashMap;

use async_trait::async_trait;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::source_as_provider;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::runtime::{PlanResolver, ResolutionResult};
use vegafusion_runtime::data::codec::VegaFusionCodec;
use vegafusion_runtime::data::external_table::ExternalTableProvider;

/// A `PlanResolver` that delegates to a Python object.
///
/// The bridge serializes the `LogicalPlan` to protobuf and calls
/// `resolve_plan_proto(bytes, datasets)` on the Python side.
pub struct PyPlanResolver {
    py_resolver: Py<PyAny>,
    has_any_override: bool,
}

impl PyPlanResolver {
    pub fn new(py_resolver: Py<PyAny>) -> Self {
        let has_any_override = Python::attach(|py| {
            let obj = py_resolver.bind(py);
            check_has_override(py, obj)
        })
        .unwrap_or(false);

        Self {
            py_resolver,
            has_any_override,
        }
    }
}

/// Check whether any of the three resolve methods are overridden on the Python object.
fn check_has_override(py: Python<'_>, obj: &Bound<PyAny>) -> PyResult<bool> {
    let base_cls = py
        .import("vegafusion.plan_resolver")?
        .getattr("PlanResolver")?;

    for method_name in &["resolve_table", "resolve_plan_proto", "resolve_plan"] {
        let obj_method = obj.get_type().getattr(*method_name)?;
        let base_method = base_cls.getattr(*method_name)?;
        if !obj_method.is(&base_method) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Walk a LogicalPlan and collect (table_name, _vf_ref_id) pairs for all
/// TableScan nodes backed by ExternalTableProvider with a `_vf_ref_id` in metadata.
fn extract_external_ref_ids(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut refs = HashMap::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Ok(provider) = source_as_provider(&scan.source) {
                if let Some(ext) = provider.as_any().downcast_ref::<ExternalTableProvider>() {
                    if let Some(ref_id) = ext.metadata().get("_vf_ref_id").and_then(|v| v.as_str())
                    {
                        refs.insert(scan.table_name.table().to_string(), ref_id.to_string());
                    }
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    refs
}

/// Build a Python dict mapping table names to resolved ExternalDataset objects.
fn build_datasets_dict<'py>(
    py: Python<'py>,
    ref_ids: &HashMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let dataset_cls = py
        .import("vegafusion.dataset")?
        .getattr("ExternalDataset")?;
    let logging = py.import("logging")?;
    let logger = logging.call_method1("getLogger", ("vegafusion.plan_resolver",))?;
    let dict = PyDict::new(py);
    for (table_name, ref_id) in ref_ids {
        let resolved = dataset_cls.call_method1("resolve_ref", (ref_id.as_str(),))?;
        if resolved.is_none() {
            logger.call_method1(
                "warning",
                (format!(
                    "ExternalDataset with _vf_ref_id '{}' for table '{}' was not found \
                     (possibly garbage-collected)",
                    ref_id, table_name
                ),),
            )?;
        } else {
            dict.set_item(table_name, resolved)?;
        }
    }
    Ok(dict)
}

#[async_trait]
impl PlanResolver for PyPlanResolver {
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        if !self.has_any_override {
            // Nothing overridden — passthrough, no Python call
            return Ok(ResolutionResult::Plan(plan));
        }

        let ref_ids = extract_external_ref_ids(&plan);

        let codec = VegaFusionCodec::new();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).map_err(|e| {
            VegaFusionError::internal(format!("Failed to serialize LogicalPlan: {e}"))
        })?;

        Python::attach(|py| {
            let datasets = build_datasets_dict(py, &ref_ids).map_err(|e| {
                VegaFusionError::internal(format!("Failed to build datasets dict: {e}"))
            })?;
            let py_bytes = PyBytes::new(py, bytes.as_ref());
            let result = self
                .py_resolver
                .call_method1(py, "resolve_plan_proto", (py_bytes, datasets))
                .map_err(|e| {
                    VegaFusionError::internal(format!("Python resolve_plan_proto failed: {e}"))
                })?;

            let result_ref = result.bind(py);

            // Check if the result is a ResolvedPlan (has .plan and .datasets attrs)
            if result_ref.hasattr("plan").unwrap_or(false)
                && result_ref.hasattr("datasets").unwrap_or(false)
            {
                // Extract plan as bytes — handles both `bytes` and protobuf `LogicalPlanNode`
                let plan_attr = result_ref.getattr("plan").map_err(|e| {
                    VegaFusionError::internal(format!(
                        "Failed to get plan attribute from ResolvedPlan: {e}"
                    ))
                })?;
                let plan_bytes: Vec<u8> = plan_attr
                    .extract()
                    .or_else(|_| {
                        plan_attr
                            .call_method0("SerializeToString")
                            .and_then(|b| b.extract())
                    })
                    .map_err(|e| {
                        VegaFusionError::internal(format!(
                            "Failed to extract plan bytes from ResolvedPlan \
                         (expected bytes or protobuf message): {e}"
                        ))
                    })?;

                let datasets_obj = result_ref.getattr("datasets").map_err(|e| {
                    VegaFusionError::internal(format!(
                        "Failed to extract datasets from ResolvedPlan: {e}"
                    ))
                })?;
                let datasets_dict: &Bound<PyDict> = datasets_obj.cast().map_err(|e| {
                    VegaFusionError::internal(format!("ResolvedPlan.datasets is not a dict: {e}"))
                })?;

                let mut sidecar: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                for (key, value) in datasets_dict.iter() {
                    let table_name: String = key.extract().map_err(|e| {
                        VegaFusionError::internal(format!(
                            "Failed to extract sidecar table name: {e}"
                        ))
                    })?;
                    let table = VegaFusionTable::from_pyarrow(py, &value).map_err(|e| {
                        VegaFusionError::internal(format!(
                            "Failed to convert sidecar table '{table_name}' to Arrow: {e}"
                        ))
                    })?;
                    sidecar.insert(table_name, table.batches);
                }

                let sidecar_codec = VegaFusionCodec::with_sidecar(sidecar);
                let ctx = datafusion::prelude::SessionContext::new();
                let resolved_plan =
                    datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec(
                        &plan_bytes,
                        &ctx.task_ctx(),
                        &sidecar_codec,
                    )
                    .map_err(|e| {
                        VegaFusionError::internal(format!(
                            "Failed to deserialize resolved plan: {e}"
                        ))
                    })?;

                Ok(ResolutionResult::Plan(resolved_plan))
            } else {
                let table = VegaFusionTable::from_pyarrow(py, result_ref).map_err(|e| {
                    VegaFusionError::internal(format!(
                        "Failed to convert Python result to Arrow table: {e}"
                    ))
                })?;
                Ok(ResolutionResult::Table(table))
            }
        })
    }
}
