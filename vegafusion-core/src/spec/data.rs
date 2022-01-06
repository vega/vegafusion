/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
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

    pub fn supported(&self) -> DependencyNodeSupported {
        if let Some(Some(format_type)) = self.format.as_ref().map(|fmt| fmt.type_.clone()) {
            if !matches!(format_type.as_str(), "csv" | "tsv" | "arrow" | "json") {
                // We don't know how to read the data, so full node is unsupported
                return DependencyNodeSupported::Unsupported;
            }
        }

        // Inline values array not supported (they should be kept on the server)
        if self.values.is_some() {
            return DependencyNodeSupported::Unsupported;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFormatSpec {
    #[serde(rename = "type")]
    pub type_: Option<String>,

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
