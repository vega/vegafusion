use async_trait::async_trait;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;

pub enum ResolutionResult {
    /// Resolver fully materialized the plan
    Table(VegaFusionTable),
    /// Resolver produced a rewritten plan for DataFusion to execute
    Plan(LogicalPlan),
}

#[async_trait]
pub trait PlanResolver: Send + Sync + 'static {
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult>;
}
