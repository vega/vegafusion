use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::runtime::{PlanResolver, ResolutionResult};

use super::util::DataFrameUtils;

/// Terminal `PlanResolver` that executes plans via DataFusion.
///
/// This is the final resolver in a `ResolverPipeline`. It always returns
/// `ResolutionResult::Table` by executing the plan against the shared
/// `SessionContext`.
pub struct DataFusionResolver {
    pub(crate) ctx: Arc<SessionContext>,
}

impl DataFusionResolver {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl PlanResolver for DataFusionResolver {
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        let table = DataFrame::new(self.ctx.state(), plan)
            .collect_to_table()
            .await?;
        Ok(ResolutionResult::Table(table))
    }
}
