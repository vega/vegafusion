/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::transform::utils::DataFrameUtils;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::SchemaRef;
use vegafusion_core::arrow::util::pretty::pretty_format_batches;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};

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
        Ok(Self { schema, batches })
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
        let batch_schema = if self.batches.is_empty() {
            None
        } else {
            Some(self.batches.get(0).unwrap().schema())
        };

        MemTable::try_new(self.schema.clone(), vec![self.batches.clone()]).unwrap_or_else(|_| {
            panic!(
                "to_memtable failure with schema {:#?} and batch schema {:?}",
                self.schema, batch_schema
            )
        })
    }

    fn to_dataframe(&self) -> Result<Arc<dyn DataFrame>> {
        let mut ctx = ExecutionContext::new();
        let provider = self.to_memtable();
        ctx.register_table("df", Arc::new(provider)).unwrap();
        ctx.table("df")
            .with_context(|| "Failed to create DataFrame".to_string())
    }
}
