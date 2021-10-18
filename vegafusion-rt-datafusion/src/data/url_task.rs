use crate::task_graph::task::TaskCall;
use vegafusion_core::proto::gen::tasks::{ScanUrlTask};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::error::{Result, VegaFusionError, ToExternalError};
use async_trait::async_trait;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::options::CsvReadOptions;
use vegafusion_core::data::table::VegaFusionTable;
use crate::data::table::VegaFusionTableUtils;
use tokio::io::AsyncReadExt;
use datafusion::logical_plan::LogicalPlanBuilder;
use datafusion::execution::dataframe_impl::DataFrameImpl;


#[async_trait]
impl TaskCall for ScanUrlTask {
    async fn call(&self, values: &[TaskValue]) -> Result<(TaskValue, Vec<TaskValue>)> {
        if let [TaskValue::Scalar(url)] = values {
            let url = url.to_string();

            // Remove any file url prefix
            let url = match url.strip_prefix("file://") {
                None => url.to_string(),
                Some(url) => url.to_string(),
            };

            let df = if url.ends_with(".csv") || url.ends_with(".tsv") {
                read_csv(url).await?
            } else if url.ends_with(".json") {
                read_json(&url, self.batch_size as usize).await?
            } else {
                return Err(VegaFusionError::internal(&format!("Invalid url file extension {}", url)));
            };

            // let table_value = TaskValue::Table(table);
            let table_value = TaskValue::Table(VegaFusionTable::from_dataframe(df).await?);
            Ok((table_value, Default::default()))

        } else {
            return Err(VegaFusionError::internal("Expected single scalar input"))
        }
    }
}


async fn read_csv(
    url: String,
) -> Result<Arc<dyn DataFrame>> {
    // Build options
    let csv_opts = if url.ends_with(".tsv") {
        CsvReadOptions::new()
            .delimiter(b'\t')
            .file_extension(".tsv")
    } else {
        CsvReadOptions::new()
    };

    let mut ctx = ExecutionContext::new();
    Ok(ctx.read_csv(url, csv_opts).await?)
}


async fn read_json(
    url: &str,
    batch_size: usize,
) -> Result<Arc<dyn DataFrame>> {
    // Read to json Value from local file or url.
    let value: serde_json::Value = if url.starts_with("http://") || url.starts_with("https://") {

        // Perform get request to collect file contents as text
        let body = reqwest::get(url).await
            .external(&format!("Failed to get URL data from {}", url))?
            .text().await
            .external("Failed to convert URL data to text")?;

        serde_json::from_str(&body)?
    } else {
        // Assume local file
        let mut file = tokio::fs::File::open(url).await
            .external(&format!("Failed to open as local file: {}", url))?;

        let mut json_str = String::new();
        file.read_to_string(&mut json_str).await
            .external("Failed to read file contents to string")?;

        serde_json::from_str(&json_str)?
    };

    VegaFusionTable::from_json(value, batch_size)?.to_dataframe()
}
