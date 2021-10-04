use std::collections::HashMap;
use serde_json::Value;
use serde::{Serialize, Deserialize};
use crate::spec::transform::TransformSpecTrait;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtentTransformSpec {
    pub field: String, // TODO: support field object

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for ExtentTransformSpec{
    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}
