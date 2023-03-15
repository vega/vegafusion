use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use std::sync::Arc;
use vegafusion_common::datatypes::to_boolean;

use crate::expression::compiler::utils::VfSimplifyInfo;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Filter {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let filter_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(&dataframe.schema_df()?),
        )?;

        // Cast filter expr to boolean
        let filter_expr = to_boolean(filter_expr, &dataframe.schema_df()?)?;

        // Simplify expression prior to evaluation
        let simplifier = ExprSimplifier::new(VfSimplifyInfo::from(dataframe.schema_df()?));
        let simplified_expr = simplifier.simplify(filter_expr)?;

        let result = dataframe.filter(simplified_expr).await?;

        Ok((result, Default::default()))
    }
}
