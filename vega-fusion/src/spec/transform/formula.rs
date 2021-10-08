use crate::spec::transform::TransformSpecTrait;
use std::collections::HashMap;
use serde_json::Value;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormulaTransformSpec {
    pub expr: String,

    #[serde(rename = "as")]
    pub as_: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FormulaTransformSpec {}
