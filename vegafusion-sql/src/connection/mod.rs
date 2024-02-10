use crate::dialect::Dialect;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};

// Use Connection publicly for the convenience of SQL connection implementors
pub use vegafusion_dataframe::connection::Connection;

#[cfg(feature = "datafusion-conn")]
pub mod datafusion_conn;

#[cfg(feature = "datafusion-conn")]
#[cfg(feature = "pyarrow")]
pub mod datafusion_py_datasource;

#[async_trait]
pub trait SqlConnection: Connection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable>;

    fn dialect(&self) -> &Dialect;

    fn to_connection(&self) -> Arc<dyn Connection>;
}

#[derive(Clone, Debug)]
pub struct DummySqlConnection {
    pub dialect: Dialect,
}

impl DummySqlConnection {
    pub fn new(dialect: Dialect) -> Self {
        Self { dialect }
    }
}

#[async_trait]
impl Connection for DummySqlConnection {
    fn id(&self) -> String {
        "dummy".to_string()
    }

    async fn tables(&self) -> Result<HashMap<String, Schema>> {
        Ok(Default::default())
    }
}

#[async_trait]
impl SqlConnection for DummySqlConnection {
    async fn fetch_query(&self, _query: &str, _schema: &Schema) -> Result<VegaFusionTable> {
        Err(VegaFusionError::sql_not_supported(
            "fetch_query not supported by DummySqlConnection",
        ))
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    fn to_connection(&self) -> Arc<dyn Connection> {
        Arc::new(self.clone())
    }
}
