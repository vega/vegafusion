use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::spec::values::{Field, SignalExpressionSpec};
use crate::spec::transform::TransformSpecTrait;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinTransformSpec {
    pub field: Field,
    pub extent: BinExtent,
    pub signal: Option<String>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    // Bin configuration parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxbins: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<f64>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub minstep: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub divide: Option<Vec<f64>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nice: Option<bool>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BinExtent {
    Value([f64; 2]),
    Signal(SignalExpressionSpec),
}

impl TransformSpecTrait for BinTransformSpec {
    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}
