use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

fn default_value() -> Option<Value> {
    Some(Value::Number(serde_json::Number::from(0)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImputeTransformSpec {
    pub field: Field,
    pub key: Field,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyvals: Option<Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<ImputeMethodSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    // Default to zero but serialize even if null
    #[serde(default = "default_value")]
    pub value: Option<Value>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl ImputeTransformSpec {
    pub fn method(&self) -> ImputeMethodSpec {
        self.method.clone().unwrap_or(ImputeMethodSpec::Value)
    }

    pub fn groupby(&self) -> Vec<Field> {
        self.groupby.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImputeMethodSpec {
    Value,
    Mean,
    Median,
    Max,
    Min,
}

impl TransformSpecTrait for ImputeTransformSpec {
    fn supported(&self) -> bool {
        self.field.as_().is_none()
            && self.key.as_().is_none()
            && self.keyvals.is_none()
            && self.method() == ImputeMethodSpec::Value
    }

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

            // Add key
            col_usage = col_usage.with_column(&unescape_field(&self.key.field()));

            // Add groupby usage
            if let Some(groupby) = self.groupby.as_ref() {
                let groupby: Vec<_> = groupby
                    .iter()
                    .map(|field| unescape_field(&field.field()))
                    .collect();
                col_usage = col_usage.union(&ColumnUsage::from(groupby.as_slice()));
            }

            // Build produced (no columns are created by impute transform)
            let produced = ColumnUsage::empty();

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
        // Keep input local datetime columns as impute passes through all input columns
        // and doesn't create an columns
        Vec::from(input_local_datetime_columns)
    }
}
