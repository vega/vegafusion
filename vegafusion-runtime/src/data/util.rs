use async_trait::async_trait;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::TableReference;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::{col, Expr, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion_functions_window::row_number::row_number;
use std::sync::Arc;
use vegafusion_common::arrow::array::RecordBatch;
use vegafusion_common::arrow::compute::concat_batches;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::ResultWithContext;

#[async_trait]
pub trait SessionContextUtils {
    async fn vegafusion_table(
        &self,
        tbl: VegaFusionTable,
    ) -> vegafusion_common::error::Result<DataFrame>;
}

#[async_trait]
impl SessionContextUtils for SessionContext {
    async fn vegafusion_table(
        &self,
        tbl: VegaFusionTable,
    ) -> vegafusion_common::error::Result<DataFrame> {
        let mem_table = MemTable::try_new(tbl.schema.clone(), vec![tbl.batches])?;

        // Based on self.read_batch()
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(mem_table)), None)?
                .build()?,
        ))
    }
}

#[async_trait]
pub trait DataFrameUtils {
    async fn collect_to_table(self) -> vegafusion_common::error::Result<VegaFusionTable>;
    async fn collect_flat(self) -> vegafusion_common::error::Result<RecordBatch>;
    async fn with_index(self, index_name: &str) -> vegafusion_common::error::Result<DataFrame>;

    /// Variant of aggregate that can handle agg expressions that include projections on top
    /// of aggregations. Also includes groupby expressions in the final result
    fn aggregate_mixed(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> vegafusion_common::error::Result<DataFrame>;
    fn alias(self, name: impl Into<TableReference>) -> vegafusion_common::error::Result<DataFrame>;
}

#[async_trait]
impl DataFrameUtils for DataFrame {
    async fn collect_to_table(self) -> vegafusion_common::error::Result<VegaFusionTable> {
        let mut arrow_schema = self.schema().inner().clone();
        let batches = self.collect().await?;
        if let Some(batch) = batches.first() {
            // use first batch schema if present
            arrow_schema = batch.schema()
        }
        VegaFusionTable::try_new(arrow_schema, batches)
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

    fn aggregate_mixed(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> vegafusion_common::error::Result<DataFrame> {
        let mut select_exprs: Vec<Expr> = Vec::new();

        // Extract pure agg expressions
        let mut agg_rewriter = PureAggRewriter::new();

        for agg_expr in aggr_expr {
            let select_expr = agg_expr.rewrite(&mut agg_rewriter)?;
            select_exprs.push(select_expr.data)
        }

        // Apply pure agg functions
        let df = self.aggregate(group_expr.clone(), agg_rewriter.pure_aggs)?;

        // Add groupby exprs to selection
        select_exprs.extend(group_expr);

        // Apply projection on top of aggs
        Ok(df.select(select_exprs)?)
    }

    fn alias(self, name: impl Into<TableReference>) -> vegafusion_common::error::Result<DataFrame> {
        let (state, plan) = self.into_parts();
        Ok(DataFrame::new(
            state,
            LogicalPlanBuilder::new(plan).alias(name)?.build()?,
        ))
    }
}

pub struct PureAggRewriter {
    pub pure_aggs: Vec<Expr>,
    pub next_id: usize,
}

impl Default for PureAggRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PureAggRewriter {
    pub fn new() -> Self {
        Self {
            pure_aggs: vec![],
            next_id: 0,
        }
    }

    fn new_agg_name(&mut self) -> String {
        let name = format!("_agg_{}", self.next_id);
        self.next_id += 1;
        name
    }
}

impl TreeNodeRewriter for PureAggRewriter {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Expr::AggregateFunction(agg) = node {
            // extract agg and replace with column
            let name = self.new_agg_name();
            self.pure_aggs
                .push(Expr::AggregateFunction(agg).alias(&name));
            Ok(Transformed::new_transformed(col(name), true))
        } else {
            // Return expr node unchanged
            Ok(Transformed::no(node))
        }
    }
}
