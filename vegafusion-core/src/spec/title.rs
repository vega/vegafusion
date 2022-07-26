use crate::spec::values::ValueOrSignalSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleSpec {
    pub text: Option<ValueOrSignalSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
