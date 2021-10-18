use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use vegafusion_core::arrow::datatypes::SchemaRef;
use crate::transform::utils::DataFrameUtils;
use vegafusion_core::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use async_trait::async_trait;


#[async_trait]
pub trait VegaFusionTableUtils {
    fn from_dataframe_blocking(value: Arc<dyn DataFrame>) -> Result<VegaFusionTable>;
    async fn from_dataframe(value: Arc<dyn DataFrame>) -> Result<VegaFusionTable>;
    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String>;
    fn to_memtable(&self) -> MemTable;
    fn to_dataframe(&self) -> Result<Arc<dyn DataFrame>>;
}


#[async_trait]
impl VegaFusionTableUtils for VegaFusionTable {
    fn from_dataframe_blocking(value: Arc<dyn DataFrame>) -> Result<Self> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.block_eval()?;
        Ok(Self { schema, batches })
    }

    async fn from_dataframe(value: Arc<dyn DataFrame>) -> Result<VegaFusionTable> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.collect().await?;
        Ok(Self { schema, batches})
    }

    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String> {
        if let Some(max_rows) = max_rows {
            pretty_format_batches(&self.head(max_rows).batches)
                .with_context(|| String::from("Failed to pretty print"))
        } else {
            pretty_format_batches(&self.batches)
                .with_context(|| String::from("Failed to pretty print"))
        }
    }

    fn to_memtable(&self) -> MemTable {
        // Unwrap is safe because we perform the MemTable validation in our try_new function
        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()]).unwrap()
    }

    fn to_dataframe(&self) -> Result<Arc<dyn DataFrame>> {
        let mut ctx = ExecutionContext::new();
        let provider = self.to_memtable();
        ctx.register_table("df", Arc::new(provider)).unwrap();
        ctx.table("df")
            .with_context(|| "Failed to create DataFrame".to_string())
    }
}
