use std::collections::HashMap;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::source_as_provider;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_arrow::PySchema;
use serde_json::Value;

use datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec;
use vegafusion_common::arrow::datatypes::SchemaRef;
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
    skip_when_no_external_tables: bool,
    thread_safe: bool,
}

impl PyPlanResolver {
    pub fn new(py_resolver: Py<PyAny>) -> Self {
        let (skip_when_no_external_tables, thread_safe) = Python::attach(|py| {
            let skip = py_resolver
                .getattr(py, "skip_when_no_external_tables")
                .and_then(|v| v.extract::<bool>(py))
                .unwrap_or(true);
            let safe = py_resolver
                .getattr(py, "thread_safe")
                .and_then(|v| v.extract::<bool>(py))
                .unwrap_or(true);
            (skip, safe)
        });

        Self {
            py_resolver,
            skip_when_no_external_tables,
            thread_safe,
        }
    }

    /// Whether this resolver is safe to call from any thread.
    pub fn thread_safe(&self) -> bool {
        self.thread_safe
    }
}

/// Info extracted from an ExternalTableProvider node in the plan.
struct ExternalTableInfo {
    schema: SchemaRef,
    protocol: Option<String>,
    source: Option<String>,
    metadata: Value,
    ref_id: Option<String>,
}

/// Walk a LogicalPlan and collect ExternalTableProvider info for each table.
fn extract_external_tables(plan: &LogicalPlan) -> HashMap<String, ExternalTableInfo> {
    let mut tables = HashMap::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Ok(provider) = source_as_provider(&scan.source) {
                if let Some(ext) = provider.as_any().downcast_ref::<ExternalTableProvider>() {
                    let ref_id = ext
                        .metadata()
                        .get("_vf_ref_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    tables.insert(
                        scan.table_name.table().to_string(),
                        ExternalTableInfo {
                            schema: ext.schema(),
                            protocol: ext.protocol().map(|s| s.to_string()),
                            source: ext.source().map(|s| s.to_string()),
                            metadata: ext.metadata().clone(),
                            ref_id,
                        },
                    );
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    tables
}

/// Build a Python dict mapping table names to reconstructed ExternalDataset objects.
///
/// Each ExternalDataset is freshly constructed from protobuf-sourced schema and
/// metadata, plus the data object recovered from the Python-side registry (if any).
fn build_datasets_dict<'py>(
    py: Python<'py>,
    tables: &HashMap<String, ExternalTableInfo>,
) -> PyResult<Bound<'py, PyDict>> {
    let dataset_cls = py
        .import("vegafusion.dataset")?
        .getattr("ExternalDataset")?;
    let logging = py.import("logging")?;
    let logger = logging.call_method1("getLogger", ("vegafusion.plan_resolver",))?;
    let dict = PyDict::new(py);

    for (table_name, info) in tables {
        // Recover data from registry if ref_id is present
        let data = if let Some(ref ref_id) = info.ref_id {
            let resolved = dataset_cls.call_method1("resolve_data", (ref_id.as_str(),))?;
            if resolved.is_none() {
                logger.call_method1(
                    "warning",
                    (format!(
                        "Data for table '{}' with _vf_ref_id '{}' was not found \
                         (possibly garbage-collected)",
                        table_name, ref_id
                    ),),
                )?;
            }
            resolved
        } else {
            py.None().into_bound(py)
        };

        // Convert schema to Python via pyo3-arrow
        let py_schema = PySchema::new(info.schema.clone()).into_pyobject(py)?;

        // Convert metadata to Python dict
        let py_metadata = pythonize::pythonize(py, &info.metadata)?;

        // Reconstruct ExternalDataset(protocol, schema, metadata, data, source)
        let kwargs = PyDict::new(py);
        kwargs.set_item("protocol", info.protocol.as_deref())?;
        kwargs.set_item("schema", py_schema)?;
        kwargs.set_item("metadata", py_metadata)?;
        kwargs.set_item("data", &data)?;
        kwargs.set_item("source", info.source.as_deref())?;
        let dataset = dataset_cls.call((), Some(&kwargs))?;
        dict.set_item(table_name, dataset)?;
    }

    Ok(dict)
}

#[async_trait]
impl PlanResolver for PyPlanResolver {
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        let tables = extract_external_tables(&plan);

        if self.skip_when_no_external_tables && tables.is_empty() {
            return Ok(ResolutionResult::Plan(plan));
        }

        let codec = VegaFusionCodec::new();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).map_err(|e| {
            VegaFusionError::internal(format!("Failed to serialize LogicalPlan: {e}"))
        })?;

        Python::attach(|py| {
            let datasets = build_datasets_dict(py, &tables).map_err(|e| {
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
                let ctx = vegafusion_runtime::datafusion::context::make_datafusion_context();
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
