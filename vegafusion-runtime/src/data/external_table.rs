use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{plan_err, Result};
use datafusion_expr::Expr;
use serde_json::Value;
use vegafusion_common::arrow::datatypes::SchemaRef;

/// A marker TableProvider representing an external data source.
///
/// Carries only a schema — cannot be executed directly.
/// Custom `PlanResolver` implementations identify these nodes via
/// `as_any().downcast_ref::<ExternalTableProvider>()` and replace them
/// with real data sources before execution.
///
/// Optionally carries arbitrary JSON metadata in [`Self::metadata`],
/// which is serialized into `custom_table_data` by [`super::codec::VegaFusionCodec`].
pub struct ExternalTableProvider {
    schema: SchemaRef,
    kind: String,
    metadata: Value,
}

impl ExternalTableProvider {
    pub fn new(schema: SchemaRef, kind: String, metadata: Value) -> Self {
        Self {
            schema,
            kind,
            metadata,
        }
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
}

impl Debug for ExternalTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalTableProvider")
            .field("kind", &self.kind)
            .field("schema", &self.schema)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[async_trait]
impl TableProvider for ExternalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let kind = self.kind();
        plan_err!(
            "ExternalTableProvider (kind: {kind}) cannot be executed directly. \
             This table represents an external data source that must be resolved \
             before execution. Set a PlanResolver on the VegaFusionRuntime to \
             handle external table references."
        )
    }
}
