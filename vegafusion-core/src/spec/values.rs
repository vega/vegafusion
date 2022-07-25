/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::expression::parser::parse;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
