use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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

    #[serde(skip_serializing_if = "Option::is_none")]
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
            && self.groupby().len() <= 1
            && self.value.is_some()
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
            let mut col_usage = ColumnUsage::from(self.field.field().as_str());

            // Add key
            col_usage = col_usage.with_column(self.key.field().as_str());

            // Add groupby usage
            if let Some(groupby) = self.groupby.as_ref() {
                let groupby: Vec<_> = groupby.iter().map(|field| field.field()).collect();
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
}
