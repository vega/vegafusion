use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use crate::spec::values::StringOrStringList;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub init: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<SignalOnSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnSpec {
    pub events: SignalOnEventSpecOrList,
    pub update: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SignalOnEventSpecOrList {
    List(Vec<SignalOnEventSpec>),
    Scalar(SignalOnEventSpec),
}

impl SignalOnEventSpecOrList {
    pub fn to_vec(&self) -> Vec<SignalOnEventSpec> {
        match self {
            SignalOnEventSpecOrList::List(event_specs) => event_specs.clone(),
            SignalOnEventSpecOrList::Scalar(event_spec) => vec![event_spec.clone()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SignalOnEventSpec {
    Signal(SignalOnSignalEvent),
    Scale(SignalOnScaleEvent),
    Source(SignalOnSourceEvent),
    Selector(String),
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnSignalEvent {
    pub signal: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnScaleEvent {
    pub scale: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnSourceEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub markname: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<StringOrStringList>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub between: Option<Vec<SignalOnEventSpec>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

