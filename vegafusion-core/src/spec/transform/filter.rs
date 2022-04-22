/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::spec::transform::TransformSpecTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Result;
use crate::expression::parser::parse;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterTransformSpec {
    pub expr: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FilterTransformSpec {
    fn supported(&self) -> bool {
        if let Ok(expr) = parse(&self.expr) {
            expr.is_supported()
        } else {
            false
        }
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let expr = parse(&self.expr)?;
        Ok(expr.input_vars())
    }
}
