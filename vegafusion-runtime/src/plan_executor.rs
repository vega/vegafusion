use async_trait::async_trait;
use datafusion::prelude::{DataFrame, SessionContext};
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::runtime::PlanExecutor;

use crate::data::util::DataFrameUtils;

#[derive(Clone)]
pub struct DataFusionPlanExecutor {
    ctx: Arc<SessionContext>,
}

impl DataFusionPlanExecutor {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl PlanExecutor for DataFusionPlanExecutor {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        let df = DataFrame::new(self.ctx.state(), plan);
        df.collect_to_table().await
    }
}
