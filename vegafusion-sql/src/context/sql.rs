use std::collections::{HashMap};
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::error::{Result, VegaFusionError};
use crate::connection::SqlDatabaseConnection;
use crate::context::Context;
use crate::dataframe::DataFrame;
use crate::dataframe::sql::SqlDataFrame;

#[derive(Clone)]
pub struct SqlContext {
    conn: Arc<dyn SqlDatabaseConnection>
}

impl SqlContext {
    pub fn new(conn: Arc<dyn SqlDatabaseConnection>) -> Self {
        Self {
            conn
        }
    }
}

impl Context for SqlContext {
    fn table(&self, name: &str) -> Result<Arc<dyn DataFrame>> {
        Ok(Arc::new(SqlDataFrame::try_new(self.conn.clone(), name)?))
    }

    fn tables(&self) -> Result<HashMap<String, Schema>> {
        self.conn.tables()
    }

    fn scan_dataframe(&self, _df: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>> {
        return Err(VegaFusionError::internal("SqlContext does not support scan_dataframe"))
    }
}
