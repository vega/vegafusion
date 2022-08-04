pub mod sql;

use std::collections::{HashMap};
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::Schema;
use crate::dataframe::DataFrame;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait Context: Send + Sync {
    fn tables(&self) -> Result<HashMap<String, Schema>>;

    fn table(&self, name: &str) -> Result<Arc<dyn DataFrame>>;
    fn scan_dataframe(&self, df: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>>;
}
