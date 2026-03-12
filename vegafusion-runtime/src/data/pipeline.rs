use std::sync::Arc;

use datafusion::prelude::SessionContext;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::proto::gen::tasks::ResolverCapabilities;
use vegafusion_core::runtime::{MergedCapabilities, ParsedUrl, PlanResolver, ResolutionResult};

use super::datafusion_resolver::DataFusionResolver;

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
}

impl ResolverPipeline {
    pub fn new(user_resolvers: Vec<Arc<dyn PlanResolver>>, ctx: Arc<SessionContext>) -> Self {
        let mut resolvers: Vec<Arc<dyn PlanResolver>> = user_resolvers;
        resolvers.push(Arc::new(DataFusionResolver::new(ctx.clone())));
        Self {
            resolvers: Arc::new(resolvers),
            ctx,
        }
    }

    /// Whether any user-supplied resolvers are registered.
    pub fn has_user_resolvers(&self) -> bool {
        self.resolvers.len() > 1
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

    /// Merge capabilities from all resolvers into a single set for planner lookups.
    pub fn merged_capabilities(&self) -> MergedCapabilities {
        MergedCapabilities::from_resolver_capabilities(
            &self
                .resolvers
                .iter()
                .map(|r| r.capabilities())
                .collect::<Vec<_>>(),
        )
    }

    /// Return a single merged `ResolverCapabilities` proto (union of all resolvers).
    /// Useful for serializing capabilities over gRPC/WASM.
    pub fn merged_resolver_capabilities(&self) -> ResolverCapabilities {
        let merged = self.merged_capabilities();
        ResolverCapabilities {
            supported_schemes: merged.supported_schemes.into_iter().collect(),
            supported_format_types: merged.supported_format_types.into_iter().collect(),
            supported_extensions: merged.supported_extensions.into_iter().collect(),
        }
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
        unreachable!("DataFusionResolver (last in chain) always returns Table")
    }
}
