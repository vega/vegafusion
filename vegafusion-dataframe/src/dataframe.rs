use crate::connection::Connection;
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    expr, window_function, BuiltInWindowFunction, Expr, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

#[async_trait]
pub trait DataFrame: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn schema(&self) -> Schema;

    fn schema_df(&self) -> Result<DFSchema> {
        Ok(DFSchema::try_from(self.schema())?)
    }

    fn connection(&self) -> Arc<dyn Connection>;

    fn fingerprint(&self) -> u64;

    async fn collect(&self) -> Result<VegaFusionTable>;

    async fn collect_flat(&self) -> Result<RecordBatch> {
        let mut arrow_schema = Arc::new(self.schema()) as SchemaRef;
        let table = self.collect().await?;
        if let Some(batch) = table.batches.get(0) {
            arrow_schema = batch.schema()
        }
        concat_batches(&arrow_schema, table.batches.as_slice())
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    async fn sort(&self, _exprs: Vec<Expr>, _limit: Option<i32>) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("sort not supported"))
    }

    async fn select(&self, _exprs: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("select not supported"))
    }

    async fn aggregate(
        &self,
        _group_exprs: Vec<Expr>,
        _aggr_exprs: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "aggregate not supported",
        ))
    }

    async fn joinaggregate(
        &self,
        _group_expr: Vec<Expr>,
        _aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "joinaggregate not supported",
        ))
    }

    async fn filter(&self, _predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("filter not supported"))
    }

    async fn limit(&self, _limit: i32) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("limit not supported"))
    }

    async fn fold(
        &self,
        _fields: &[String],
        _value_col: &str,
        _key_col: &str,
        _order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("fold not supported"))
    }

    async fn stack(
        &self,
        _field: &str,
        _orderby: Vec<Expr>,
        _groupby: &[String],
        _start_field: &str,
        _stop_field: &str,
        _mode: StackMode,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("stack not supported"))
    }

    async fn impute(
        &self,
        _field: &str,
        _value: ScalarValue,
        _key: &str,
        _groupby: &[String],
        _order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("impute not supported"))
    }

    async fn with_index(&self, index_name: &str) -> Result<Arc<dyn DataFrame>> {
        if self.schema().column_with_name(index_name).is_some() {
            // Column is already present, don't overwrite
            self.select(vec![Expr::Wildcard]).await
        } else {
            let selections = vec![
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
                        end_bound: WindowFrameBound::CurrentRow,
                    },
                })
                .alias(index_name),
                Expr::Wildcard,
            ];
            self.select(selections).await
        }
    }
}

#[derive(Debug, Clone)]
pub enum StackMode {
    Zero,
    Center,
    Normalize,
}

impl Display for StackMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StackMode::Zero => write!(f, "zero"),
            StackMode::Center => write!(f, "center"),
            StackMode::Normalize => write!(f, "normalize"),
        }
    }
}
