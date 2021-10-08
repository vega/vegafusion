use serde::{Deserialize, Serialize};

use crate::error::{Result, ResultWithContext};
use crate::expression::ast::base::Expression;
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::parser::parse;

use crate::spec::transform::formula::FormulaTransformSpec;
use crate::transform::base::TransformTrait;
use crate::variable::Variable;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

/// Compiled representation for the filter transform spec
#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct FormulaTransform {
    expr: Expression,
    input_vars: Vec<Variable>,
    as_: String,
}

impl FormulaTransform {
    pub fn try_new(spec: &FormulaTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        let input_vars = expr.get_variables();
        Ok(Self {
            expr,
            input_vars,
            as_: spec.as_.clone(),
        })
    }
}

impl TransformTrait for FormulaTransform {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let formula_expr = compile(&self.expr, config, Some(dataframe.schema()))?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.as_);

        let result = dataframe
            .select(vec![Expr::Wildcard, formula_expr])
            .with_context(|| format!("Formula transform failed with expression: {}", self.expr))?;

        Ok((result, Default::default()))
    }

    fn input_vars(&self) -> Vec<Variable> {
        self.input_vars.clone()
    }
}
