use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

/// Struct that serializes to Vega spec for the fold transform
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldTransformSpec {
    pub fields: Vec<Field>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl FoldTransformSpec {
    pub fn as_(&self) -> Vec<String> {
        let as_ = self.as_.clone().unwrap_or_default();
        vec![
            as_.get(0).cloned().unwrap_or_else(|| "key".to_string()),
            as_.get(1).cloned().unwrap_or_else(|| "value".to_string()),
        ]
    }
}

impl TransformSpecTrait for FoldTransformSpec {
    fn supported(&self) -> bool {
        !self.fields.is_empty()
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            // Produces "as" columns
            let produced = ColumnUsage::from(self.as_().as_slice());

            // Uses fields columns
            let unescaped_fields = self
                .fields
                .iter()
                .map(|field| unescape_field(&field.field()))
                .collect::<Vec<_>>();
            let col_usage = ColumnUsage::from(unescaped_fields.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);

            // All input columns are passed through as well
            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns
        let mut local_datetime_cols = Vec::from(input_local_datetime_columns);

        // Add the "value" column if all the fields columns are local datetime columns
        let value_is_datetime = self
            .fields
            .iter()
            .all(|field| local_datetime_cols.contains(&field.field()));

        if value_is_datetime {
            let value_col = self.as_().get(1).cloned().unwrap();
            local_datetime_cols.push(value_col)
        }

        local_datetime_cols
    }
}
