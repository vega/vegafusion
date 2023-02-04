use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

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
            let fields: Vec<_> = self
                .fields
                .iter()
                .map(|field| unescape_field(&field.field()))
                .collect();
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

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns as lookup passes through all input columns,
        // Note: lookup may introduce additional datetime columns that we're not capturing here
        Vec::from(input_local_datetime_columns)
    }
}
