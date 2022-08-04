pub mod sql;

use std::any::Any;
use std::sync::Arc;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, VegaFusionError};
use async_trait::async_trait;
use crate::context::{Context};

#[async_trait]
pub trait DataFrame: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn sort(&self, columns: &[&str]) -> Result<Arc<dyn DataFrame>>;
    async fn collect(&self) -> Result<VegaFusionTable>;
}
