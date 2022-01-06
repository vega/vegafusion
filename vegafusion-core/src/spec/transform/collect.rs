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
