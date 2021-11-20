use crate::spec::values::SignalExpressionSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<ScaleDomainSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<ScaleRangeSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bins: Option<ScaleBinsSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDomainSpec {
    Reference(ScaleDataReferenceSpec),
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleDataReferenceSpec {
    pub data: String,
    pub field: String,

    // Need to support sort objects as well as booleans
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub sort: Option<bool>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleArrayElementSpec {
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleBinsSpec {
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleRangeSpec {
    Reference(ScaleDataReferenceSpec),
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}
