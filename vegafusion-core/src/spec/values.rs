use crate::error::Result;
use crate::expression::parser::parse;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl Default for StringOrStringList {
    fn default() -> Self {
        Self::StringList(Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldObject {
    pub field: String,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalExpressionSpec {
    pub signal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrSignalSpec {
    String(String),
    Signal(SignalExpressionSpec),
}

impl StringOrSignalSpec {
    pub fn input_vars(&self) -> Result<Vec<InputVariable>> {
        match self {
            Self::Signal(signal) => Ok(parse(&signal.signal)?.input_vars()),
            _ => Ok(Default::default()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NumberOrSignalSpec {
    Number(f64),
    Signal(SignalExpressionSpec),
}

impl NumberOrSignalSpec {
    pub fn input_vars(&self) -> Result<Vec<InputVariable>> {
        match self {
            Self::Signal(signal) => Ok(parse(&signal.signal)?.input_vars()),
            _ => Ok(Default::default()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueOrSignalSpec {
    Signal(SignalExpressionSpec),
    Value(serde_json::Value),
}

impl ValueOrSignalSpec {
    pub fn input_vars(&self) -> Result<Vec<InputVariable>> {
        match self {
            Self::Signal(signal) => Ok(parse(&signal.signal)?.input_vars()),
            _ => Ok(Default::default()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompareSpec {
    pub field: StringOrStringList,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrderOrList>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SortOrderSpec {
    Descending,
    Ascending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortOrderOrList {
    SortOrder(SortOrderSpec),
    SortOrderList(Vec<SortOrderSpec>),
}

impl SortOrderOrList {
    pub fn to_vec(&self) -> Vec<SortOrderSpec> {
        match self {
            SortOrderOrList::SortOrder(v) => vec![v.clone()],
            SortOrderOrList::SortOrderList(v) => v.clone(),
        }
    }
}

/// Helper struct that will not drop null values on round trip (de)serialization
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MissingNullOrValue {
    #[default]
    Missing,
    Null,
    Value(serde_json::Value),
}

impl MissingNullOrValue {
    pub fn is_missing(&self) -> bool {
        matches!(self, MissingNullOrValue::Missing)
    }

    pub fn as_option(&self) -> Option<serde_json::Value> {
        match self {
            MissingNullOrValue::Missing => None,
            MissingNullOrValue::Null => Some(serde_json::Value::Null),
            MissingNullOrValue::Value(v) => Some(v.clone()),
        }
    }
}

impl From<Option<serde_json::Value>> for MissingNullOrValue {
    fn from(opt: Option<serde_json::Value>) -> MissingNullOrValue {
        match opt {
            Some(v) => MissingNullOrValue::Value(v),
            None => MissingNullOrValue::Null,
        }
    }
}

impl<'de> Deserialize<'de> for MissingNullOrValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::deserialize(deserializer).map(Into::into)
    }
}

impl Serialize for MissingNullOrValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MissingNullOrValue::Missing => None::<Option<serde_json::Value>>.serialize(serializer),
            MissingNullOrValue::Null => serde_json::Value::Null.serialize(serializer),
            MissingNullOrValue::Value(v) => v.serialize(serializer),
        }
    }
}
