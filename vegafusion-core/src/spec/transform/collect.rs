/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::CompareSpec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectTransformSpec {
    pub sort: CompareSpec,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for CollectTransformSpec {}
