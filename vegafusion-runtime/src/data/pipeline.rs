use std::sync::Arc;

use datafusion::prelude::SessionContext;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::runtime::{PlanResolver, ResolutionResult};

use super::datafusion_resolver::DataFusionResolver;

/// Chains user-supplied resolvers with a terminal `DataFusionResolver`.
///
/// Each user resolver either returns a fully materialized `Table` (short-circuiting
/// the pipeline) or a rewritten `Plan` that is passed to the next resolver.
/// The `DataFusionResolver` at the end always executes the plan and returns a table.
#[derive(Clone)]
pub struct ResolverPipeline {
    user_resolvers: Arc<Vec<Arc<dyn PlanResolver>>>,
    datafusion_resolver: Arc<DataFusionResolver>,
}

impl ResolverPipeline {
    pub fn new(user_resolvers: Vec<Arc<dyn PlanResolver>>, ctx: Arc<SessionContext>) -> Self {
        Self {
            user_resolvers: Arc::new(user_resolvers),
            datafusion_resolver: Arc::new(DataFusionResolver::new(ctx)),
        }
    }

    /// Whether any user-supplied resolvers are registered.
    pub fn has_user_resolvers(&self) -> bool {
        !self.user_resolvers.is_empty()
    }

    /// Access the shared `SessionContext`.
    pub fn ctx(&self) -> &SessionContext {
        &self.datafusion_resolver.ctx
    }

    /// Resolve a `LogicalPlan` to a `VegaFusionTable`.
    ///
    /// Iterates through user resolvers first; if any returns `Table`, that result
    /// is returned immediately. Otherwise the (possibly rewritten) plan is executed
    /// by the terminal `DataFusionResolver`.
    pub async fn resolve(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        let mut current = plan;
        for resolver in self.user_resolvers.iter() {
            match resolver.resolve_plan(current).await? {
                ResolutionResult::Table(table) => return Ok(table),
                ResolutionResult::Plan(p) => current = p,
            }
        }
        // Terminal: DataFusionResolver always returns Table
        match self.datafusion_resolver.resolve_plan(current).await? {
            ResolutionResult::Table(table) => Ok(table),
            ResolutionResult::Plan(_) => unreachable!("DataFusionResolver always returns Table"),
        }
    }
}
