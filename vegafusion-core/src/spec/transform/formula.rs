/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::expression::parser::parse;
use crate::spec::transform::TransformSpecTrait;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FormulaTransformSpec {
    pub expr: String,

    #[serde(rename = "as")]
    pub as_: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FormulaTransformSpec {
    fn supported(&self) -> bool {
        if let Ok(expr) = parse(&self.expr) {
            // Create nested field by using a dot in the field name is not supported yet
            expr.is_supported() && !self.as_.contains('.')
        } else {
            false
        }
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let expr = parse(&self.expr)?;
        Ok(expr.input_vars())
    }
}
