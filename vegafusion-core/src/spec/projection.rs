use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSpec {
    pub name: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
