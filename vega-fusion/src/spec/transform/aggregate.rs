use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateTransformSpec {
    pub groupby: Vec<Field>,
    pub fields: Vec<Option<Field>>,
    pub ops: Vec<AggregateOp>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cross: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Field>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    Count,
    Valid,
    Missing,
    Distinct,
    Sum,
    Product,
    Mean,
    Average,
    Variance,
    Variancp,
    Stdev,
    Stdevp,
    Stderr,
    Median,
    Q1,
    Q3,
    Ci0,
    Ci1,
    Min,
    Max,
    Argmin,
    Argmax,
    Values,
}

impl AggregateOp {
    pub fn name(&self) -> String {
        serde_json::to_value(self)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }
}

impl TransformSpecTrait for AggregateTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOp::*;
        for op in &self.ops {
            if !matches!(
                op,
                Count | Valid | Missing | Distinct | Sum | Mean | Average | Min | Max
            ) {
                // Unsupported aggregation op
                return false;
            }
        }

        // Cross aggregation not supported
        if let Some(true) = &self.cross {
            return false;
        }

        // drop=false not support
        if let Some(false) = &self.drop {
            return false;
        }
        true
    }
}
