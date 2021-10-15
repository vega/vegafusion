use crate::data::table::VegaFusionTable;
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
use vegafusion_core::variable::Variable;

impl TransformTrait for Formula {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let formula_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.r#as);

        println!("formula_expr: {}", formula_expr);
        println!("schema: {}", dataframe.schema());

        let explained = VegaFusionTable::try_from(dataframe.explain(true, false).unwrap()).unwrap();
        println!("explained\n{}", explained.pretty_format(None).unwrap());

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

    fn input_vars(&self) -> Vec<Variable> {
        self.expr.as_ref().unwrap().get_variables()
    }
}
