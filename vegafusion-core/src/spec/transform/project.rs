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
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectTransformSpec {
    pub fields: Vec<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for ProjectTransformSpec {
    fn supported(&self) -> bool {
        true
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        Ok(Default::default())
    }
}
