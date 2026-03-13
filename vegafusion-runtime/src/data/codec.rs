use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion_common::{not_impl_err, DataFusionError, Result, TableReference};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use serde_json::Value;
use vegafusion_common::arrow::datatypes::SchemaRef;
use vegafusion_common::arrow::record_batch::RecordBatch;

use super::external_table::ExternalTableProvider;
use super::inline_table::InlineTableProvider;

/// Codec for serializing/deserializing [`ExternalTableProvider`] and
/// [`InlineTableProvider`] via DataFusion's proto.
///
/// When serializing a `LogicalPlan` containing these provider nodes,
/// they become `CustomTableScanNode` in the protobuf output with:
/// - `table_name`: the logical table name
/// - `schema`: full Arrow schema (handled automatically by DataFusion)
/// - `custom_table_data`: JSON-encoded envelope
///
/// The optional `sidecar` carries inline table data (keyed by table_id)
/// that is injected during deserialization to reconstruct `MemTable` instances.
#[derive(Debug, Default)]
pub struct VegaFusionCodec {
    sidecar: Option<HashMap<String, Vec<RecordBatch>>>,
}

impl VegaFusionCodec {
    pub fn new() -> Self {
        Self { sidecar: None }
    }

    pub fn with_sidecar(sidecar: HashMap<String, Vec<RecordBatch>>) -> Self {
        Self {
            sidecar: Some(sidecar),
        }
    }
}

impl LogicalExtensionCodec for VegaFusionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::execution::TaskContext,
    ) -> Result<datafusion_expr::Extension> {
        not_impl_err!("VegaFusionCodec does not support extension nodes")
    }

    fn try_encode(&self, _node: &datafusion_expr::Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("VegaFusionCodec does not support extension nodes")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &datafusion::execution::TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if buf.is_empty() {
            // Backward compatibility: empty buf treated as ExternalTableProvider
            return Ok(Arc::new(ExternalTableProvider::new(
                schema,
                None,
                Value::Null,
            )));
        }

        let envelope: Value = serde_json::from_slice(buf).map_err(|e| {
            DataFusionError::Plan(format!("Failed to decode table provider envelope: {e}"))
        })?;

        match envelope.get("type").and_then(|t| t.as_str()) {
            Some("external") => {
                let protocol = envelope
                    .get("protocol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let source = envelope
                    .get("source")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let metadata = envelope.get("metadata").cloned().unwrap_or(Value::Null);
                Ok(Arc::new(
                    ExternalTableProvider::new(schema, protocol, metadata).with_source(source),
                ))
            }
            Some("inline") => {
                let name = envelope
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "InlineTableProvider envelope missing 'name' field".to_string(),
                        )
                    })?;

                let sidecar = self.sidecar.as_ref().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "No sidecar provided to decode InlineTableProvider '{name}'"
                    ))
                })?;

                let batches = sidecar.get(name).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Sidecar does not contain entry for InlineTableProvider '{name}'"
                    ))
                })?;

                let mem_table = MemTable::try_new(schema, vec![batches.clone()])?;
                Ok(Arc::new(mem_table))
            }
            Some(other) => Err(DataFusionError::Plan(format!(
                "Unknown table provider type in envelope: '{other}'"
            ))),
            None => {
                // No "type" field — treat as legacy ExternalTableProvider where
                // the entire JSON value is the metadata
                Ok(Arc::new(ExternalTableProvider::new(schema, None, envelope)))
            }
        }
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if let Some(ext) = node.as_any().downcast_ref::<ExternalTableProvider>() {
            let mut envelope = serde_json::json!({
                "type": "external",
                "protocol": ext.protocol(),
                "metadata": ext.metadata(),
            });
            if let Some(source) = ext.source() {
                envelope["source"] = serde_json::Value::String(source.to_string());
            }
            let json_bytes = serde_json::to_vec(&envelope).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to encode ExternalTableProvider envelope: {e}"
                ))
            })?;
            buf.extend_from_slice(&json_bytes);
            Ok(())
        } else if let Some(inline) = node.as_any().downcast_ref::<InlineTableProvider>() {
            let envelope = serde_json::json!({
                "type": "inline",
                "name": inline.table_id(),
            });
            let json_bytes = serde_json::to_vec(&envelope).map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to encode InlineTableProvider envelope: {e}"
                ))
            })?;
            buf.extend_from_slice(&json_bytes);
            Ok(())
        } else {
            not_impl_err!(
                "VegaFusionCodec only supports ExternalTableProvider and InlineTableProvider, got {:?}",
                node
            )
        }
    }
}
