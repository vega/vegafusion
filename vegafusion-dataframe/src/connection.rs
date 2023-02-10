use crate::csv::CsvReadOptions;
use crate::dataframe::DataFrame;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};

#[async_trait]
pub trait Connection: Send + Sync + 'static {
    fn id(&self) -> String;

    /// Name and schema of the tables that are provided by this connection
    async fn tables(&self) -> Result<HashMap<String, Schema>>;

    /// Scan a named tabel into a DataFrame
    async fn scan_table(&self, _name: &str) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_table not supported by connection",
        ))
    }

    /// Scan a VegaFusionTable into a DataFrame
    async fn scan_arrow(&self, _table: VegaFusionTable) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_arrow not supported by connection",
        ))
    }

    /// Scan a CSV file into a DataFrame
    async fn scan_csv(&self, _path: &str, _opts: CsvReadOptions) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_csv not supported by connection",
        ))
    }
}
