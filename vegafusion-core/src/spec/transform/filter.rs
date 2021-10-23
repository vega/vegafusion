use crate::spec::transform::TransformSpecTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::proto::gen::tasks::Variable;
use crate::expression::parser::parse;
use crate::error::Result;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterTransformSpec {
    pub expr: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FilterTransformSpec {
    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let expr = parse(&self.expr)?;
        Ok(expr.input_vars())
    }
}
