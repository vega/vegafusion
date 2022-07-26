/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::CompareSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectTransformSpec {
    pub sort: CompareSpec,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for CollectTransformSpec {
    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            let col_usage = ColumnUsage::from(self.sort.field.to_vec().as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::PassThrough {
                usage,
                produced: ColumnUsage::empty(),
            }
        } else {
            TransformColumns::Unknown
        }
    }
}
