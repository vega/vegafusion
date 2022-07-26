/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::values::{SignalExpressionSpec, SortOrderSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDomainSpec {
    Array(Vec<ScaleArrayElementSpec>),
    FieldReference(ScaleDataReferenceSpec),
    FieldsReference(ScaleDataReferencesSpec),
    FieldsVecStrings(ScaleVecStringsSpec),
    FieldsStrings(ScaleStringsSpec),
    FieldsSignals(ScaleSignalsSpec),
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleDataReferencesSpec {
    pub fields: Vec<ScaleDataReferenceSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleVecStringsSpec {
    pub fields: Vec<Vec<String>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleStringsSpec {
    pub fields: Vec<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleSignalsSpec {
    pub fields: Vec<SignalExpressionSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleDataReferenceSortParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<AggregateOpSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrderSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleArrayElementSpec {
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleBinsSpec {
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleRangeSpec {
    Reference(ScaleDataReferenceSpec),
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}
