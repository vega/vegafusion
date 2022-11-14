/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentifierTransformSpec {
    #[serde(rename = "as")]
    pub as_: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for IdentifierTransformSpec {
    fn supported(&self) -> bool {
        true
    }

    fn transform_columns(
        &self,
        _datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        let usage = DatasetsColumnUsage::empty();
        let produced = ColumnUsage::from(self.as_.as_str());
        TransformColumns::PassThrough { usage, produced }
    }
}
