use std::sync::Arc;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_expr::Expr;
use datafusion_expr::expr::WildcardOptions;
use datafusion_functions_window::row_number::row_number;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::compute::concat_batches;
use vegafusion_common::arrow::datatypes::SchemaRef;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};

#[async_trait]
pub trait SessionContextUtils {
    async fn vegafusion_table(&self, tbl: VegaFusionTable) -> vegafusion_common::error::Result<DataFrame>;
}

#[async_trait]
impl SessionContextUtils for SessionContext {
    async fn vegafusion_table(&self, tbl: VegaFusionTable) -> vegafusion_common::error::Result<DataFrame> {
        let mem_table = MemTable::try_new(tbl.schema.clone(), vec![tbl.batches])?;
        self.register_table("tbl", Arc::new(mem_table))?;
        Ok(self.table("tbl").await?)
    }
}


#[async_trait]
pub trait DataFrameUtils {
    async fn collect_to_table(self) -> vegafusion_common::error::Result<VegaFusionTable>;
    async fn collect_flat(self) -> vegafusion_common::error::Result<RecordBatch>;
    async fn with_index(self, index_name: &str) -> vegafusion_common::error::Result<DataFrame>;
}


#[async_trait]
impl DataFrameUtils for DataFrame {
    async fn collect_to_table(self) -> vegafusion_common::error::Result<VegaFusionTable> {
        let schema = self.schema().inner().clone();
        let batches = self.collect().await?;
        VegaFusionTable::try_new(schema, batches)
    }

    async fn collect_flat(self) -> vegafusion_common::error::Result<RecordBatch> {
        let mut arrow_schema = self.schema().inner().clone();
        let batches = self.collect().await?;
        if let Some(batch) = batches.first() {
            arrow_schema = batch.schema()
        }
        concat_batches(&arrow_schema, batches.as_slice())
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    async fn with_index(self, index_name: &str) -> vegafusion_common::error::Result<DataFrame> {
        if self.schema().inner().column_with_name(index_name).is_some() {
            // Column is already present, don't overwrite
            Ok(self.select(vec![Expr::Wildcard {
                qualifier: None,
                options: WildcardOptions::default(),
            }])?)
        } else {
            let selections = vec![
                row_number().alias(index_name),
                Expr::Wildcard {
                    qualifier: None,
                    options: WildcardOptions::default(),
                },
            ];
            Ok(self.select(selections)?)
        }
    }
}
