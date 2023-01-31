pub mod datafusion_conn;
pub mod sqlite_conn;

use async_trait::async_trait;
use sqlgen::dialect::Dialect;
use std::collections::HashMap;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;

#[async_trait]
pub trait SqlConnection: Send + Sync {
    fn id(&self) -> String;

    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable>;

    async fn tables(&self) -> Result<HashMap<String, Schema>>;

    fn dialect(&self) -> &Dialect;
}
