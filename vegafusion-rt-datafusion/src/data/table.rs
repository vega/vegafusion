use crate::expression::compiler::call::make_session_context;
use crate::sql::connection::datafusion_conn::DataFusionConnection;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::utils::DataFrameUtils;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::SchemaRef;
use vegafusion_core::arrow::util::pretty::pretty_format_batches;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};

#[async_trait]
pub trait VegaFusionTableUtils {
    fn from_dataframe_blocking(value: DataFrame) -> Result<VegaFusionTable>;
    async fn from_dataframe(value: DataFrame) -> Result<VegaFusionTable>;
    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String>;
    fn to_memtable(&self) -> MemTable;
    async fn to_dataframe(&self) -> Result<DataFrame>;
    async fn to_sql_dataframe(&self) -> Result<Arc<SqlDataFrame>>;
}

#[async_trait]
impl VegaFusionTableUtils for VegaFusionTable {
    fn from_dataframe_blocking(value: DataFrame) -> Result<Self> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.block_eval()?;
        Ok(Self { schema, batches })
    }

    async fn from_dataframe(value: DataFrame) -> Result<VegaFusionTable> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.collect().await?;
        Ok(Self { schema, batches })
    }

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

    fn to_memtable(&self) -> MemTable {
        // Unwrap is safe because we perform the MemTable validation in our try_new function
        let batch_schema = if self.batches.is_empty() {
            None
        } else {
            Some(self.batches.get(0).unwrap().schema())
        };

        MemTable::try_new(
            batch_schema.clone().unwrap_or_else(|| self.schema.clone()),
            vec![self.batches.clone()],
        )
        .unwrap_or_else(|_| {
            panic!(
                "to_memtable failure with schema {:#?} and batch schema {:#?}",
                self.schema, batch_schema
            )
        })
    }

    async fn to_dataframe(&self) -> Result<DataFrame> {
        let ctx = SessionContext::new();
        let provider = self.to_memtable();
        ctx.register_table("df", Arc::new(provider)).unwrap();
        ctx.table("df")
            .await
            .with_context(|| "Failed to create DataFrame".to_string())
    }

    async fn to_sql_dataframe(&self) -> Result<Arc<SqlDataFrame>> {
        let ctx = make_session_context();
        ctx.register_table("tbl", Arc::new(self.to_memtable()))?;
        let sql_conn = DataFusionConnection::new(Arc::new(ctx));
        Ok(Arc::new(
            SqlDataFrame::try_new(Arc::new(sql_conn), "tbl").await?,
        ))
    }
}
