use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

/// Struct that serializes to Vega spec for the filter transform
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PivotTransformSpec {
    pub field: String,
    pub value: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<AggregateOpSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for PivotTransformSpec {
    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            // Unknown produced columns
            let produced = ColumnUsage::Unknown;

            // Compute used columns (groupby, value, and pivot)
            let mut usage_cols: Vec<_> = self
                .groupby
                .clone()
                .unwrap_or_default()
                .iter()
                .map(|f| unescape_field(f))
                .collect();
            usage_cols.push(unescape_field(&self.field.clone()));
            usage_cols.push(unescape_field(&self.value.clone()));
            let col_usage = ColumnUsage::from(usage_cols.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::Overwrite { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns that are used as grouping fields
        self.groupby
            .clone()
            .unwrap_or_default()
            .iter()
            .filter_map(|groupby_field| {
                if input_local_datetime_columns.contains(groupby_field) {
                    Some(groupby_field.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
