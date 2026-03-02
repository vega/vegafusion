use async_trait::async_trait;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;

#[async_trait]
pub trait PlanExecutor: Send + Sync {
    fn name(&self) -> &str;
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable>;
}
