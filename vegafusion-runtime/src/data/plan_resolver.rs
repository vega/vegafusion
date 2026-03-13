use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::{provider_as_source, source_as_provider, MemTable};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlan as DFLogicalPlan;
use vegafusion_common::arrow::datatypes::SchemaRef;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::tasks::ResolverCapabilities;
use vegafusion_core::runtime::{ParsedUrl, ResolutionResult};

use super::external_table::ExternalTableProvider;

#[async_trait]
pub trait PlanResolver: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Declare what URL patterns this resolver supports at planning time.
    /// Returns empty capabilities by default (no additional URL support).
    fn capabilities(&self) -> ResolverCapabilities {
        ResolverCapabilities::default()
    }

    /// Given a parsed URL, optionally return a LogicalPlan to handle it.
    /// Return Ok(None) to pass the URL to the next resolver in the chain.
    async fn scan_url(&self, _parsed_url: &ParsedUrl) -> Result<Option<LogicalPlan>> {
        Ok(None)
    }

    /// Provide data for a single external table reference.
    ///
    /// Called once per `ExternalTableProvider` node in the plan.
    /// Override this instead of [`resolve_plan`](Self::resolve_plan) when
    /// each external table can be resolved independently.
    ///
    /// The default `resolve_plan` walks the plan tree and calls this method
    /// for every `ExternalTableProvider` it finds, replacing each with an
    /// in-memory table.
    ///
    /// # Arguments
    /// * `name` - table name from the plan
    /// * `scheme` - URL scheme identifier (e.g. `"spark"`, `"snowflake"`)
    /// * `schema` - full Arrow schema of the external table
    /// * `metadata` - JSON metadata from ExternalTableProvider
    /// * `projected_columns` - column names DataFusion actually needs,
    ///   or `None` if all columns are needed
    async fn resolve_table(
        &self,
        _name: &str,
        _scheme: &str,
        _schema: SchemaRef,
        _metadata: &serde_json::Value,
        _projected_columns: Option<Vec<String>>,
    ) -> Result<VegaFusionTable> {
        Err(VegaFusionError::internal(
            "resolve_table not implemented — override resolve_table or resolve_plan",
        ))
    }

    /// Resolve a LogicalPlan containing external table references.
    ///
    /// The default implementation walks the plan tree, finds
    /// `ExternalTableProvider` nodes, calls [`resolve_table`](Self::resolve_table)
    /// for each, and replaces them with in-memory table scans. Plans with
    /// no external tables are passed through unchanged.
    ///
    /// Override this for full control over plan rewriting (e.g. SQL
    /// transpilation or remote execution).
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        let external_tables = extract_external_tables(&plan);

        if external_tables.is_empty() {
            return Ok(ResolutionResult::Plan(plan));
        }

        // Resolve each external table, then wrap as MemTable
        let mut mem_tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        for (table_name, info) in &external_tables {
            let table = self
                .resolve_table(
                    table_name,
                    &info.scheme,
                    info.schema.clone(),
                    &info.metadata,
                    info.projected_columns.clone(),
                )
                .await?;
            let mem_table =
                MemTable::try_new(table.schema.clone(), vec![table.batches]).map_err(|e| {
                    VegaFusionError::internal(format!("Failed to create MemTable: {e}"))
                })?;
            mem_tables.insert(table_name.clone(), Arc::new(mem_table));
        }

        // Rewrite the plan, replacing ExternalTableProvider with MemTable
        let mut rewriter = ResolvedTableRewriter { tables: mem_tables };
        let rewritten = plan
            .rewrite(&mut rewriter)
            .map_err(|e| VegaFusionError::internal(format!("Failed to rewrite plan: {e}")))?
            .data;

        Ok(ResolutionResult::Plan(rewritten))
    }
}

/// Info extracted from an ExternalTableProvider node in a LogicalPlan.
struct ExternalTableInfo {
    scheme: String,
    schema: SchemaRef,
    metadata: serde_json::Value,
    projected_columns: Option<Vec<String>>,
}

/// Walk a LogicalPlan and collect ExternalTableProvider info for each table scan.
fn extract_external_tables(plan: &LogicalPlan) -> HashMap<String, ExternalTableInfo> {
    let mut tables = HashMap::new();
    let _ = plan.apply(|node| {
        if let DFLogicalPlan::TableScan(scan) = node {
            if let Ok(provider) = source_as_provider(&scan.source) {
                if let Some(ext) = provider.as_any().downcast_ref::<ExternalTableProvider>() {
                    let projected_columns = scan.projection.as_ref().map(|indices| {
                        let schema = ext.schema();
                        indices
                            .iter()
                            .map(|&i| schema.field(i).name().clone())
                            .collect()
                    });
                    tables.insert(
                        scan.table_name.table().to_string(),
                        ExternalTableInfo {
                            scheme: ext.scheme().to_string(),
                            schema: ext.schema(),
                            metadata: ext.metadata().clone(),
                            projected_columns,
                        },
                    );
                }
            }
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    tables
}

/// Rewriter that replaces ExternalTableProvider scans with MemTable scans.
struct ResolvedTableRewriter {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl TreeNodeRewriter for ResolvedTableRewriter {
    type Node = DFLogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let DFLogicalPlan::TableScan(scan) = &node {
            let table_name = scan.table_name.table();
            if let Some(mem_table) = self.tables.get(table_name) {
                let new_scan = DFLogicalPlan::TableScan(datafusion_expr::TableScan {
                    table_name: scan.table_name.clone(),
                    source: provider_as_source(mem_table.clone()),
                    projection: scan.projection.clone(),
                    projected_schema: scan.projected_schema.clone(),
                    filters: scan.filters.clone(),
                    fetch: scan.fetch,
                });
                return Ok(Transformed::yes(new_scan));
            }
        }
        Ok(Transformed::no(node))
    }
}
