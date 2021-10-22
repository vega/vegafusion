use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Formula;
use vegafusion_core::data::table::VegaFusionTable;
use crate::data::table::VegaFusionTableUtils;
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
impl TransformTrait for Formula {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let formula_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.r#as);

        let result = dataframe
            .select(vec![Expr::Wildcard, formula_expr])
            .with_context(|| {
                format!(
                    "Formula transform failed with expression: {}",
                    &self.expr.as_ref().unwrap()
                )
            })?;

        Ok((result, Default::default()))
    }
}
