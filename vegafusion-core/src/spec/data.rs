use crate::spec::transform::TransformSpec;
use crate::spec::values::StringOrSignalSpec;
use itertools::sorted;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

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

    pub fn supported(&self) -> DataSupported {
        // TODO: also add checks for supported file formats, etc.
        let all_supported = self.transform.iter().all(|tx| tx.supported());
        if all_supported {
            DataSupported::Supported
        } else if self.url.is_some() {
            DataSupported::PartiallySupported
        } else {
            match self.transform.get(0) {
                Some(tx) if tx.supported() => DataSupported::PartiallySupported,
                _ => DataSupported::Unsupported,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub enum DataSupported {
    Supported,
    PartiallySupported,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFormatSpec {
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse: Option<DataFormatParseSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataFormatParseSpec {
    Auto(String),
    Object(HashMap<String, String>),
}
