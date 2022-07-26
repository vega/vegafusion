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
pub struct ExtentTransformSpec {
    pub field: String, // TODO: support field object

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for ExtentTransformSpec {
    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            let usage = DatasetsColumnUsage::empty()
                .with_column_usage(datum_var, ColumnUsage::empty().with_column(&self.field));
            TransformColumns::PassThrough {
                usage,
                produced: ColumnUsage::empty(),
            }
        } else {
            TransformColumns::Unknown
        }
    }
}
