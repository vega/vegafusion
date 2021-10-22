use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrStringList {
    String(String),
    StringList(Vec<String>),
}

impl StringOrStringList {
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            StringOrStringList::String(v) => vec![v.clone()],
            StringOrStringList::StringList(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Field {
    String(String),
    Object(FieldObject),
}

impl Field {
    pub fn field(&self) -> String {
        match self {
            Field::String(field) => field.clone(),
            Field::Object(FieldObject { field, .. }) => field.clone(),
        }
    }

    pub fn as_(&self) -> Option<String> {
        match self {
            Field::String(_) => None,
            Field::Object(FieldObject { as_, .. }) => as_.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldObject {
    pub field: String,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalExpressionSpec {
    pub signal: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrSignalSpec {
    String(String),
    Signal(SignalExpressionSpec),
}