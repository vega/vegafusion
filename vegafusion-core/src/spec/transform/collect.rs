use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::{CompareSpec, SortOrderOrList, StringOrStringList};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectTransformSpec {
    pub sort: CompareSpec,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for CollectTransformSpec {}
