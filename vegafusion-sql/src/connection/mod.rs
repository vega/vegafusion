// pub mod datafusion_conn;
// pub mod sqlite_conn;

use crate::dialect::Dialect;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::Result;
use vegafusion_dataframe::connection::Connection;

#[cfg(feature = "datafusion-conn")]
pub mod datafusion_conn;

#[cfg(feature = "sqlite-conn")]
pub mod sqlite_conn;

#[async_trait]
pub trait SqlConnection: Connection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable>;

    fn dialect(&self) -> &Dialect;

    fn to_connection(&self) -> Arc<dyn Connection>;
}
