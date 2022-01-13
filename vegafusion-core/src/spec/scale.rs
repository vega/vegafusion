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
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::values::{SignalExpressionSpec, SortOrderSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<ScaleTypeSpec>,

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
#[serde(rename_all = "lowercase")]
pub enum ScaleTypeSpec {
    // Quantitative Scales
    Linear,
    Log,
    Pow,
    Sqrt,
    Symlog,
    Time,
    Utc,
    Sequential,

    // Discrete Scales
    Ordinal,
    Band,
    Point,

    // Discretizing Scales
    Quantile,
    Quantize,
    Threshold,
    #[serde(rename = "bin-ordinal")]
    BinOrdinal,
}

impl Default for ScaleTypeSpec {
    fn default() -> Self {
        Self::Linear
    }
}

impl ScaleTypeSpec {
    pub fn is_discrete(&self) -> bool {
        use ScaleTypeSpec::*;
        matches!(self, Ordinal | Band | Point)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDomainSpec {
    FieldReference(ScaleDataReferenceSpec),
    FieldsReference(ScaleDataReferencesSpec),
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleDataReferencesSpec {
    pub fields: Vec<ScaleDataReferenceSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleDataReferenceSpec {
    pub data: String,
    pub field: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<ScaleDataReferenceSort>,

    // Need to support sort objects as well as booleans
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub sort: Option<bool>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDataReferenceSort {
    Bool(bool),
    Parameters(ScaleDataReferenceSortParameters),
}

impl Default for ScaleDataReferenceSort {
    fn default() -> Self {
        Self::Bool(false)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScaleDataReferenceSortParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<AggregateOpSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrderSpec>,
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
