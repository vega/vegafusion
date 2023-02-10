use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

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
            let unescaped_sort_fields = self
                .sort
                .field
                .to_vec()
                .iter()
                .map(|f| unescape_field(f))
                .collect::<Vec<_>>();
            let col_usage = ColumnUsage::from(unescaped_sort_fields.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::PassThrough {
                usage,
                produced: ColumnUsage::empty(),
            }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns as collect passes through all input columns
        // and doesn't create an columns
        Vec::from(input_local_datetime_columns)
    }
}
