use async_trait::async_trait;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;

pub enum ResolutionResult {
    /// Resolver fully materialized the plan
    Table(VegaFusionTable),
    /// Resolver produced a rewritten plan for the next resolver to handle,
    /// or for DataFusion to execute if this is the last resolver
    Plan(LogicalPlan),
}

#[async_trait]
pub trait PlanResolver: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult>;
}
