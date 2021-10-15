use crate::transform::TransformTrait;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use crate::expression::compiler::config::CompilationConfig;
use datafusion::scalar::ScalarValue;
use crate::expression::compiler::compile;
use vegafusion_core::error::{Result, ResultWithContext};
use datafusion::logical_plan::Expr;
use vegafusion_core::variable::Variable;
use vegafusion_core::proto::gen::transforms::Formula;
use crate::data::table::VegaFusionTable;
use std::convert::TryFrom;


impl TransformTrait for Formula {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let formula_expr = compile(&self.expr.as_ref().unwrap(), config, Some(dataframe.schema()))?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.r#as);

        println!("formula_expr: {}", formula_expr);
        println!("schema: {}", dataframe.schema());

        let explained = VegaFusionTable::try_from(dataframe.explain(true, false).unwrap()).unwrap();
        println!("explained\n{}", explained.pretty_format(None).unwrap());

        let result = dataframe
            .select(vec![Expr::Wildcard, formula_expr])
            .with_context(|| format!("Formula transform failed with expression: {}", &self.expr.as_ref().unwrap()))?;

        Ok((result, Default::default()))
    }

    fn input_vars(&self) -> Vec<Variable> {
        self.expr.as_ref().unwrap().get_variables()
    }
}
