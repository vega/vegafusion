use std::collections::HashMap;
use serde_json::Value;
use serde::{Serialize, Deserialize};
use crate::spec::transform::TransformSpecTrait;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterTransformSpec {
    pub expr: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FilterTransformSpec {}
