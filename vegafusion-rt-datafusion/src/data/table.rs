/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
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
    fn from_dataframe_blocking(value: Arc<DataFrame>) -> Result<VegaFusionTable>;
    async fn from_dataframe(value: Arc<DataFrame>) -> Result<VegaFusionTable>;
    fn pretty_format(&self, max_rows: Option<usize>) -> Result<String>;
    fn to_memtable(&self) -> MemTable;
    fn to_dataframe(&self) -> Result<Arc<DataFrame>>;
}

#[async_trait]
impl VegaFusionTableUtils for VegaFusionTable {
    fn from_dataframe_blocking(value: Arc<DataFrame>) -> Result<Self> {
        let schema: SchemaRef = Arc::new(value.schema().into()) as SchemaRef;
        let batches = value.block_eval()?;
        Ok(Self { schema, batches })
    }

    async fn from_dataframe(value: Arc<DataFrame>) -> Result<VegaFusionTable> {
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

    fn to_dataframe(&self) -> Result<Arc<DataFrame>> {
        let ctx = SessionContext::new();
        let provider = self.to_memtable();
        ctx.register_table("df", Arc::new(provider)).unwrap();
        ctx.table("df")
            .with_context(|| "Failed to create DataFrame".to_string())
    }
}
