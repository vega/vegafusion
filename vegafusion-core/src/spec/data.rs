use crate::spec::transform::TransformSpec;
use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use crate::spec::values::StringOrSignalSpec;
use itertools::sorted;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<StringOrSignalSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<DataFormatSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transform: Vec<TransformSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub on: Option<Value>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl DataSpec {
    pub fn output_signals(&self) -> Vec<String> {
        let mut signals: HashSet<String> = Default::default();

        for tx in &self.transform {
            signals.extend(tx.output_signals())
        }

        sorted(signals).into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFormatSpec {
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
