use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::expression::ast::base::Expression;
use crate::variable::Variable;
use crate::transform::base::TransformTrait;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use crate::expression::compiler::config::CompilationConfig;
use crate::error::Result;
use crate::expression::parser::parse;
use crate::expression::compiler::compile;
use datafusion::scalar::ScalarValue;
use crate::spec::transform::filter::FilterTransformSpec;

/// Compiled representation for the filter transform spec
#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct FilterTransform {
    expr: Expression,
    input_vars: Vec<Variable>,
}

impl FilterTransform {
    pub fn try_new(spec: &FilterTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        let input_vars = expr.get_variables();
        Ok(Self {
            expr, input_vars
        })
    }
}


impl TransformTrait for FilterTransform {
    fn call(&self, dataframe: Arc<dyn DataFrame>, config: &CompilationConfig) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let logical_expr = compile(&self.expr, config, Some(dataframe.schema()))?;
        let result = dataframe.filter(logical_expr)?;
        Ok((result, Default::default()))
    }

    fn input_vars(&self) -> Vec<Variable> {
        self.input_vars.clone()
    }
}