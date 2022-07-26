/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::spec::axis::AxisSpec;
use crate::spec::chart::{ChartVisitor, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::scale::ScaleSpec;
use crate::spec::signal::SignalSpec;
use crate::spec::title::TitleSpec;
use crate::spec::values::StringOrStringList;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkSpec {
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<MarkFromSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<MarkSort>,

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

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub axes: Vec<AxisSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transform: Vec<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<TitleSpec>,

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
        for axis in &self.axes {
            visitor.visit_axis(axis, &scope)?;
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

    pub fn walk_mut(&mut self, visitor: &mut dyn MutChartVisitor, scope: &[u32]) -> Result<()> {
        // Top-level
        let scope = Vec::from(scope);
        for data in &mut self.data {
            visitor.visit_data(data, &scope)?;
        }
        for scale in &mut self.scales {
            visitor.visit_scale(scale, &scope)?;
        }
        for axis in &mut self.axes {
            visitor.visit_axis(axis, &scope)?;
        }
        for signal in &mut self.signals {
            visitor.visit_signal(signal, &scope)?;
        }
        let mut group_index = 0;
        for mark in &mut self.marks {
            if mark.type_ == "group" {
                let mut nested_scope = scope.clone();
                nested_scope.push(group_index);

                visitor.visit_group_mark(mark, &nested_scope)?;
                mark.walk_mut(visitor, &nested_scope)?;

                group_index += 1;
            } else {
                // Keep parent scope
                visitor.visit_non_group_mark(mark, &scope)?;
            }
        }

        Ok(())
    }

    pub fn get_group(&self, group_index: u32) -> Result<&MarkSpec> {
        self.marks
            .iter()
            .filter(|m| m.type_ == "group")
            .nth(group_index as usize)
            .with_context(|| format!("No group with index {}", group_index))
    }

    pub fn get_group_mut(&mut self, group_index: u32) -> Result<&mut MarkSpec> {
        self.marks
            .iter_mut()
            .filter(|m| m.type_ == "group")
            .nth(group_index as usize)
            .with_context(|| format!("No group with index {}", group_index))
    }

    pub fn get_nested_group_mut(&mut self, path: &[u32]) -> Result<&mut MarkSpec> {
        if path.is_empty() {
            return Err(VegaFusionError::internal("Path may not be empty"));
        }
        let mut group = self.get_group_mut(path[0])?;
        for group_index in &path[1..] {
            group = group.get_group_mut(*group_index)?;
        }
        Ok(group)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkEncodeSpec {
    // e.g. enter, update, hover, etc.
    #[serde(flatten)]
    pub encodings: HashMap<String, MarkEncodingsSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkEncodingsSpec {
    // e.g. x, fill, width, etc.
    #[serde(flatten)]
    pub channels: HashMap<String, MarkEncodingOrList>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MarkEncodingOrList {
    List(Vec<MarkEncodingSpec>),
    Scalar(Box<MarkEncodingSpec>),
}

impl MarkEncodingOrList {
    pub fn to_vec(&self) -> Vec<MarkEncodingSpec> {
        match self {
            MarkEncodingOrList::List(m) => m.clone(),
            MarkEncodingOrList::Scalar(m) => vec![m.as_ref().clone()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkFromSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet: Option<MarkFacetSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkFacetSpec {
    pub data: String,
    pub name: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MarkEncodingField {
    Field(String),
    Object(MarkEncodingFieldObject),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkEncodingFieldObject {
    pub signal: Option<String>,
    pub datum: Option<String>,
    pub group: Option<String>,
    pub parent: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkSort {
    pub field: StringOrStringList,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}
