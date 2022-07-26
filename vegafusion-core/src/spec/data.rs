/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */

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

    pub fn supported(&self, extract_inline_data: bool) -> DependencyNodeSupported {
        if let Some(Some(format_type)) = self.format.as_ref().map(|fmt| fmt.type_.clone()) {
            if !matches!(format_type.as_str(), "csv" | "tsv" | "arrow" | "json") {
                // We don't know how to read the data, so full node is unsupported
                return DependencyNodeSupported::Unsupported;
            }
        }

        // Check if inline values array is supported
        if let Some(values) = &self.values {
            if !extract_inline_data {
                return DependencyNodeSupported::Unsupported;
            }
            if let Value::Array(values) = values {
                if values.is_empty() {
                    // Empty data not supported
                    return DependencyNodeSupported::Unsupported;
                }
            } else {
                // Non-array data not supported
                return DependencyNodeSupported::Unsupported;
            }
        }

        let all_supported = self.transform.iter().all(|tx| tx.supported());
        if all_supported {
            DependencyNodeSupported::Supported
        } else if self.url.is_some() {
            DependencyNodeSupported::PartiallySupported
        } else {
            match self.transform.get(0) {
                Some(tx) if tx.supported() => DependencyNodeSupported::PartiallySupported,
                _ => DependencyNodeSupported::Unsupported,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub enum DependencyNodeSupported {
    Supported,
    PartiallySupported,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFormatSpec {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse: Option<DataFormatParseSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataFormatParseSpec {
    Auto(String),
    Object(HashMap<String, String>),
}
