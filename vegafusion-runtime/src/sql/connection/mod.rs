pub mod datafusion_conn;
pub mod sqlite_conn;

use crate::sql::dataframe::DataFrame;
use crate::sql::dialect::Dialect;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::Schema;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};

/// Options that control the reading of CSV files.
/// Simplification of CsvReadOptions from DataFusion
#[derive(Clone, Debug)]
pub struct CsvReadOptions {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<Schema>,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::CSV.get_ext().as_str()`.
    pub file_extension: String,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema: None,
            file_extension: ".csv".to_string(),
        }
    }
}

#[async_trait]
pub trait Connection: Send + Sync + 'static {
    fn id(&self) -> String;

    async fn tables(&self) -> Result<HashMap<String, Schema>>;

    async fn scan_table(&self, _name: &str) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_table not supported by connection",
        ))
    }

    async fn scan_arrow(&self, _table: VegaFusionTable) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_arrow not supported by connection",
        ))
    }

    async fn scan_csv(&self, _path: &str, _opts: CsvReadOptions) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "scan_csv not supported by connection",
        ))
    }
}

#[async_trait]
pub trait SqlConnection: Connection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable>;

    fn dialect(&self) -> &Dialect;

    fn to_connection(&self) -> Arc<dyn Connection>;
}
