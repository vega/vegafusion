pub mod sqlite;

use std::collections::HashMap;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;
use async_trait::async_trait;
use sqlgen::ast::Query;


#[async_trait]
pub trait SqlDatabaseConnection: Send + Sync {
    async fn fetch_query(
        &self,
        query: &Query,
        schema: &Schema
    ) -> Result<VegaFusionTable>;

    fn tables(&self) -> Result<HashMap<String, Schema>>;
}