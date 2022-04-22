/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinAggregateTransformSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    pub fields: Vec<Option<Field>>,
    pub ops: Vec<AggregateOpSpec>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for JoinAggregateTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOpSpec::*;
        for op in &self.ops {
            if !matches!(
                op,
                Count | Valid | Missing | Distinct | Sum | Mean | Average | Min | Max
            ) {
                // Unsupported aggregation op
                return false;
            }
        }

        true
    }
}
