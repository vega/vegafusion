use async_trait::async_trait;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};

#[async_trait]
pub trait PlanExecutor: Send + Sync {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable>;
}

/// A no-op implementation of PlanExecutor that always returns an error
/// This is useful for contexts where a PlanExecutor is required but plan execution is not expected
#[derive(Debug, Clone, Default)]
pub struct NoOpPlanExecutor;

#[async_trait]
impl PlanExecutor for NoOpPlanExecutor {
    async fn execute_plan(&self, _plan: LogicalPlan) -> Result<VegaFusionTable> {
        Err(VegaFusionError::internal(
            "NoOpPlanExecutor cannot execute logical plans. Provide a concrete PlanExecutor (e.g., DataFusionPlanExecutor).",
        ))
    }
}
