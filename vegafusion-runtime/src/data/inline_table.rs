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
use vegafusion_common::arrow::datatypes::SchemaRef;

/// A marker TableProvider representing an inline data table.
///
/// Carries a schema and a table_id that can be used to look up the
/// actual data at deserialization time via the codec's sidecar.
/// Cannot be executed directly — the codec replaces these with MemTable
/// instances during deserialization.
pub struct InlineTableProvider {
    schema: SchemaRef,
    table_id: String,
}

impl InlineTableProvider {
    pub fn new(schema: SchemaRef, table_id: String) -> Self {
        Self { schema, table_id }
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }
}

impl Debug for InlineTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineTableProvider")
            .field("schema", &self.schema)
            .field("table_id", &self.table_id)
            .finish()
    }
}

#[async_trait]
impl TableProvider for InlineTableProvider {
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
        plan_err!(
            "InlineTableProvider cannot be executed directly — resolve via VegaFusionCodec sidecar"
        )
    }
}
