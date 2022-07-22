/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Result;
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            let col_usage = ColumnUsage::from(self.fields.as_slice());
            let usage =
                DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage.clone());
            TransformColumns::Overwrite {
                usage,
                produced: col_usage,
            }
        } else {
            TransformColumns::Unknown
        }
    }
}
