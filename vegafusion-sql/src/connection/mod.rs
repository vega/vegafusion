pub mod sqlite;

use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;
use async_trait::async_trait;
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::SessionContext;
use sqlgen::ast::Query;
use sqlgen::dialect::Dialect;


#[async_trait]
pub trait SqlDatabaseConnection: Send + Sync {
    async fn fetch_query(
        &self,
        query: &str,
        schema: &Schema
    ) -> Result<VegaFusionTable>;

    fn tables(&self) -> Result<HashMap<String, Schema>>;

    fn dialect(&self) -> &Dialect;

    fn session_context(&self) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        for (table_name, schema) in self.tables()? {
            let table = EmptyTable::new(Arc::new(schema));
            ctx.register_table(table_name.as_str(), Arc::new(table));
        }
        Ok(ctx)
    }
}
