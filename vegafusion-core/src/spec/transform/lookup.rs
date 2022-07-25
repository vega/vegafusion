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

use crate::proto::gen::tasks::Variable;
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the lookup transform.
/// This is currently only needed to report the proper input dependencies
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupTransformSpec {
    pub from: String,

    pub fields: Vec<Field>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for LookupTransformSpec {
    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        Ok(vec![InputVariable {
            var: Variable::new_data(&self.from),
            propagate: true,
        }])
    }

    fn supported(&self) -> bool {
        false
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            // For lookup, we can tell which field(s) are needed, but we don't currently
            // attempt to determine the output fields
            let fields: Vec<_> = self.fields.iter().map(|field| field.field()).collect();
            let usage = DatasetsColumnUsage::empty()
                .with_column_usage(datum_var, ColumnUsage::from(fields.as_slice()));

            TransformColumns::PassThrough {
                usage,
                produced: ColumnUsage::Unknown,
            }
        } else {
            TransformColumns::Unknown
        }
    }
}
