use std::collections::{HashMap};
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::error::Result;
use crate::connection::SqlDatabaseConnection;
use crate::dataframe::SqlDataFrame;

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

    fn table(&self, name: &str) -> Result<Arc<SqlDataFrame>> {
        Ok(Arc::new(SqlDataFrame::try_new(self.conn.clone(), name)?))
    }

    fn tables(&self) -> Result<HashMap<String, Schema>> {
        self.conn.tables()
    }
}
