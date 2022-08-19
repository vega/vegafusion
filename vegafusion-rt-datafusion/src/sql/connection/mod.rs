pub mod datafusion_conn;
pub mod sqlite_conn;

use async_trait::async_trait;
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;

use crate::expression::compiler::call::make_session_context;
use sqlgen::dialect::Dialect;

#[async_trait]
pub trait SqlConnection: Send + Sync {
    fn id(&self) -> String;

    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable>;

    async fn tables(&self) -> Result<HashMap<String, Schema>>;

    fn dialect(&self) -> &Dialect;

    async fn session_context(&self) -> Result<SessionContext> {
        let ctx = make_session_context();
        for (table_name, schema) in self.tables().await? {
            let table = EmptyTable::new(Arc::new(schema));
            ctx.register_table(table_name.as_str(), Arc::new(table))?;
        }
        Ok(ctx)
    }
}
