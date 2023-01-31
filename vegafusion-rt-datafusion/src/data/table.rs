use async_trait::async_trait;
use vegafusion_core::arrow::util::pretty::pretty_format_batches;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};

#[async_trait]
pub trait VegaFusionTableUtils {
    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String>;
}

#[async_trait]
impl VegaFusionTableUtils for VegaFusionTable {
    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String> {
        if let Some(max_rows) = max_rows {
            pretty_format_batches(&self.head(max_rows).batches)
                .with_context(|| String::from("Failed to pretty print"))
                .map(|s| s.to_string())
        } else {
            pretty_format_batches(&self.batches)
                .with_context(|| String::from("Failed to pretty print"))
                .map(|s| s.to_string())
        }
    }
}
