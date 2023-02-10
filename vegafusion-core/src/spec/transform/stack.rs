use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::{CompareSpec, Field};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StackTransformSpec {
    pub field: Field,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<CompareSpec>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<StackOffsetSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl StackTransformSpec {
    pub fn as_(&self) -> Vec<String> {
        self.as_
            .clone()
            .unwrap_or_else(|| vec!["y0".to_string(), "y1".to_string()])
    }

    pub fn offset(&self) -> StackOffsetSpec {
        self.offset.clone().unwrap_or(StackOffsetSpec::Zero)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StackOffsetSpec {
    Zero,
    Center,
    Normalize,
}

impl TransformSpecTrait for StackTransformSpec {
    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            // Init column usage with field
            let mut col_usage = ColumnUsage::from(unescape_field(&self.field.field()).as_str());

            // Add groupby usage
            if let Some(groupby) = self.groupby.as_ref() {
                let groupby: Vec<_> = groupby
                    .iter()
                    .map(|field| unescape_field(&field.field()))
                    .collect();
                col_usage = col_usage.union(&ColumnUsage::from(groupby.as_slice()));
            }

            // Add sort usage
            if let Some(compares) = self.sort.as_ref() {
                let unescaped_sort_fields: Vec<_> = compares
                    .field
                    .to_vec()
                    .iter()
                    .map(|f| unescape_field(f))
                    .collect();
                col_usage = col_usage.union(&ColumnUsage::from(unescaped_sort_fields.as_slice()));
            }

            // Build produced
            let produced = ColumnUsage::from(self.as_().as_slice());

            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns as stack passes through all input columns and will
        // never create a local datetime column.
        Vec::from(input_local_datetime_columns)
    }
}
