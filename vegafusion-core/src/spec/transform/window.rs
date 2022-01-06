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
use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::{CompareSpec, Field};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowTransformSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<CompareSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    pub ops: Vec<WindowTransformOpSpec>,

    pub fields: Vec<Option<Field>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<Value>>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<[Value; 2]>,

    #[serde(rename = "ignorePeers", skip_serializing_if = "Option::is_none")]
    pub ignore_peers: Option<bool>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum WindowOpSpec {
    #[serde(rename = "row_number")]
    RowNumber,
    Rank,

    #[serde(rename = "dense_rank")]
    DenseRank,

    #[serde(rename = "percent_rank")]
    PercentileRank,

    #[serde(rename = "cume_dist")]
    CumeDist,
    NTile,
    Lag,
    Lead,

    #[serde(rename = "first_value")]
    FirstValue,

    #[serde(rename = "last_value")]
    LastValue,

    #[serde(rename = "nth_value")]
    NthValue,

    #[serde(rename = "prev_value")]
    PrevValue,

    #[serde(rename = "next_value")]
    NextValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WindowTransformOpSpec {
    Aggregate(AggregateOpSpec),
    Window(WindowOpSpec),
}

impl TransformSpecTrait for WindowTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOpSpec::*;
        use WindowOpSpec::*;
        for op in &self.ops {
            match op {
                WindowTransformOpSpec::Aggregate(op) => {
                    if !matches!(op, Count | Sum | Mean | Average | Min | Max | Values) {
                        // Unsupported aggregation op
                        return false;
                    }
                }
                WindowTransformOpSpec::Window(op) => {
                    if !matches!(
                        op,
                        RowNumber
                            | Rank
                            | DenseRank
                            | PercentileRank
                            | CumeDist
                            | FirstValue
                            | LastValue
                    ) {
                        // Unsupported window op
                        return false;
                    }
                }
            }
        }

        // Custom window frames are not yet supported in DataFusion
        // https://github.com/apache/arrow-datafusion/issues/361
        // Until they are supported, the default frame is equivalent to [null, 0]. Fortunately,
        // this is the default in vega as well.
        if self.frame.is_some() && self.frame.as_ref().unwrap() != &[Value::Null, Value::from(0)] {
            return false;
        }

        true
    }
}
