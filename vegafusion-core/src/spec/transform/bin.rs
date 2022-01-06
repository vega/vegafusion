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
use crate::error::Result;
use crate::expression::parser::parse;

use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::{Field, SignalExpressionSpec};
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

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
    pub span: Option<BinSpan>,

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BinSpan {
    Value(f64),
    Signal(SignalExpressionSpec),
}

impl TransformSpecTrait for BinTransformSpec {
    fn supported(&self) -> bool {
        // Check extent expression
        if let BinExtent::Signal(extent) = &self.extent {
            if let Ok(expression) = parse(&extent.signal) {
                if !expression.is_supported() {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check span expression
        if let Some(BinSpan::Signal(span)) = &self.span {
            if let Ok(expression) = parse(&span.signal) {
                if !expression.is_supported() {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let mut input_vars: HashSet<InputVariable> = HashSet::new();
        if let BinExtent::Signal(extent) = &self.extent {
            let expression = parse(&extent.signal)?;
            input_vars.extend(expression.input_vars())
        }

        if let Some(BinSpan::Signal(span)) = &self.span {
            let expression = parse(&span.signal)?;
            input_vars.extend(expression.input_vars())
        }

        Ok(input_vars.into_iter().collect())
    }

    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}
