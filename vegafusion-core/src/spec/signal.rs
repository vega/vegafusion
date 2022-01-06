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
use crate::expression::parser::parse;
use crate::spec::data::DependencyNodeSupported;
use crate::spec::values::StringOrStringList;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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

impl SignalSpec {
    pub fn supported(&self) -> DependencyNodeSupported {
        if self.value.is_some() {
            return DependencyNodeSupported::Supported;
        } else if let Some(expr) = &self.update {
            if self.on.is_empty() {
                if let Ok(expression) = parse(expr) {
                    if expression.is_supported() {
                        return DependencyNodeSupported::Supported;
                    }
                }
            }
        }
        // TODO: add init once we decide how to differentiate it from update in task graph
        DependencyNodeSupported::Unsupported
    }
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
