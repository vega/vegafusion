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
use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::Field;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateTransformSpec {
    pub groupby: Vec<Field>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<Option<Field>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<Vec<AggregateOpSpec>>,

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
pub enum AggregateOpSpec {
    Count,
    Valid,
    Missing,
    Distinct,
    Sum,
    Product,
    Mean,
    Average,
    Variance,
    Variancep,
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

impl AggregateOpSpec {
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
        use AggregateOpSpec::*;
        let ops = self.ops.clone().unwrap_or_else(|| vec![Count]);
        for op in &ops {
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
