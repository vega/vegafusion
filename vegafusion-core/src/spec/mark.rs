

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::{HashMap, HashSet};
use crate::spec::data::DataSpec;
use crate::spec::signal::SignalSpec;
use crate::spec::scale::ScaleSpec;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkSpec {
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<MarkFromSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode: Option<MarkEncodeSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<DataSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<SignalSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub marks: Vec<MarkSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scales: Vec<ScaleSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkEncodeSpec {
    // e.g. enter, update, hover, etc.
    #[serde(flatten)]
    pub encodings: HashMap<String, MarkEncodingsSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkEncodingsSpec {
    // e.g. x, fill, width, etc.
    #[serde(flatten)]
    pub channels: HashMap<String, MarkEncodingOrList>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MarkEncodingOrList {
    List(Vec<MarkEncodingSpec>),
    Scalar(MarkEncodingSpec),
}

impl MarkEncodingOrList {
    pub fn to_vec(&self) -> Vec<MarkEncodingSpec> {
        match self {
            MarkEncodingOrList::List(m) => m.clone(),
            MarkEncodingOrList::Scalar(m) => vec![m.clone()],
        }
    }
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkEncodingSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<MarkEncodingField>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub band: Option<Number>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub test: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkFromSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet: Option<MarkFacetSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkFacetSpec {
    data: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MarkEncodingField {
    Field(String),
    Object(Value),
}
