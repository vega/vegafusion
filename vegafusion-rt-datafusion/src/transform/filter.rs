use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
use vegafusion_core::variable::Variable;

impl TransformTrait for Filter {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let logical_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;
        let result = dataframe.filter(logical_expr)?;
        Ok((result, Default::default()))
    }

    fn input_vars(&self) -> Vec<Variable> {
        self.expr.as_ref().unwrap().get_variables()
    }
}
