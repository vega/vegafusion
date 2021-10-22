

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::{HashMap, HashSet};
use crate::spec::data::DataSpec;
use crate::spec::signal::SignalSpec;
use crate::spec::scale::ScaleSpec;
use crate::spec::chart::ChartVisitor;
use crate::error::Result;


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

impl MarkSpec {
    pub fn walk(&self, visitor: &mut dyn ChartVisitor, scope: &[u32]) -> Result<()> {
        // Top-level
        let scope = Vec::from(scope);
        for data in &self.data {
            visitor.visit_data(data, &scope)?;
        }
        for scale in &self.scales {
            visitor.visit_scale(scale, &scope)?;
        }
        for signal in &self.signals {
            visitor.visit_signal(signal, &scope)?;
        }
        let mut group_index = 0;
        for mark in &self.marks {
            if mark.type_ == "group" {
                let mut nested_scope = scope.clone();
                nested_scope.push(group_index);

                visitor.visit_group_mark(mark, &nested_scope)?;
                mark.walk(visitor, &nested_scope)?;

                group_index += 1;
            } else {
                // Keep parent scope
                visitor.visit_non_group_mark(mark, &scope)?;
            }
        }

        Ok(())
    }
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
