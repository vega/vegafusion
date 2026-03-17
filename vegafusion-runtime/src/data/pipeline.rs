use std::sync::Arc;

use datafusion::datasource::source_as_provider;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::LogicalPlan as DFLogicalPlan;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::data::url::normalize_base_url;
use vegafusion_core::runtime::{ParsedUrl, ResolutionResult};

use super::datafusion_resolver::DataFusionResolver;
use super::external_table::ExternalTableProvider;
use super::plan_resolver::PlanResolver;

/// CDN base URL for vega-datasets, used as the default data_base_url.
pub const VEGA_DATASETS_CDN_BASE: &str =
    "https://raw.githubusercontent.com/vega/vega-datasets/v2.3.0/";

/// Three-state base URL setting for public API boundaries.
#[derive(Clone, Debug, Default)]
pub enum DataBaseUrlSetting {
    /// Use the default CDN base URL (vega-datasets)
    #[default]
    Default,
    /// Disable base URL; relative paths produce an error
    Disabled,
    /// Use a custom base URL (scheme URL or absolute path)
    Custom(String),
}

/// Map a `DataBaseUrlSetting` to the two-state `Option<String>` used internally.
/// Custom base URLs are normalized (bare absolute paths become file:// URLs).
pub fn resolve_data_base_url(setting: &DataBaseUrlSetting) -> Result<Option<String>> {
    match setting {
        DataBaseUrlSetting::Default => Ok(Some(VEGA_DATASETS_CDN_BASE.to_string())),
        DataBaseUrlSetting::Disabled => Ok(None),
        DataBaseUrlSetting::Custom(s) => Ok(Some(normalize_base_url(s.clone())?)),
    }
}

/// Chains resolvers with a terminal `DataFusionResolver`.
///
/// All resolvers (user-supplied + DataFusionResolver) live in a single vec.
/// DataFusionResolver is always the last resolver in the chain.
///
/// For `scan_url`, resolvers are tried in order; the first `Some(plan)` wins.
/// For `resolve`, each resolver either returns a `Table` (short-circuiting)
/// or a rewritten `Plan` passed to the next resolver.
#[derive(Clone)]
pub struct ResolverPipeline {
    resolvers: Arc<Vec<Arc<dyn PlanResolver>>>,
    ctx: Arc<SessionContext>,
    data_base_url: Option<String>,
}

impl ResolverPipeline {
    pub fn new(
        user_resolvers: Vec<Arc<dyn PlanResolver>>,
        ctx: Arc<SessionContext>,
        data_base_url: Option<String>,
    ) -> Self {
        let mut resolvers: Vec<Arc<dyn PlanResolver>> = user_resolvers;
        resolvers.push(Arc::new(DataFusionResolver::new(ctx.clone())));
        Self {
            resolvers: Arc::new(resolvers),
            ctx,
            data_base_url,
        }
    }

    /// The resolved data base URL, used for resolving relative URLs at eval time.
    pub fn data_base_url(&self) -> &Option<String> {
        &self.data_base_url
    }

    /// Whether the runtime should eagerly materialize a `LogicalPlan` into
    /// an in-memory Arrow table.
    ///
    /// Materializes when:
    /// 1. All resolvers support in-memory Arrow tables, OR
    /// 2. The plan contains no `ExternalTableProvider` nodes (no resolver
    ///    will need to intercept it)
    ///
    /// Keeps the plan lazy otherwise, so resolvers that need plan-level
    /// access (e.g. a Spark connector) can intercept external tables.
    pub fn should_materialize(&self, plan: &LogicalPlan) -> bool {
        if self.resolvers.iter().all(|r| r.supports_arrow_tables()) {
            return true;
        }
        !has_external_table_scans(plan)
    }

    /// Access the shared `SessionContext`.
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Try each resolver's `scan_url` in order. Returns the first `Some(plan)`.
    pub async fn scan_url(&self, parsed_url: &ParsedUrl) -> Result<Option<LogicalPlan>> {
        for resolver in self.resolvers.iter() {
            if let Some(plan) = resolver.scan_url(parsed_url).await? {
                return Ok(Some(plan));
            }
        }
        Ok(None)
    }

    /// Resolve a `LogicalPlan` to a `VegaFusionTable`.
    ///
    /// Iterates through all resolvers; if any returns `Table`, that result
    /// is returned immediately. Otherwise the (possibly rewritten) plan is
    /// passed to the next resolver.
    pub async fn resolve(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        let mut current = plan;
        for resolver in self.resolvers.iter() {
            match resolver.resolve_plan(current).await? {
                ResolutionResult::Table(table) => return Ok(table),
                ResolutionResult::Plan(p) => current = p,
            }
        }
        Err(VegaFusionError::internal(
            "No resolver produced a final table",
        ))
    }
}

/// Returns true if the plan contains any `ExternalTableProvider` table scans.
fn has_external_table_scans(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let DFLogicalPlan::TableScan(scan) = node {
            if let Ok(provider) = source_as_provider(&scan.source) {
                if provider
                    .as_any()
                    .downcast_ref::<ExternalTableProvider>()
                    .is_some()
                {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
