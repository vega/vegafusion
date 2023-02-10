use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

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
            let unescaped_fields: Vec<_> = self.fields.iter().map(|f| unescape_field(f)).collect();
            let col_usage = ColumnUsage::from(unescaped_fields.as_slice());
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

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns that are used as projection fields
        self.fields
            .iter()
            .filter_map(|project_field| {
                if input_local_datetime_columns.contains(project_field) {
                    Some(project_field.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
