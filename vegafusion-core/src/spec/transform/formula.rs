use crate::error::Result;
use crate::expression::column_usage::{ColumnUsage, GetDatasetsColumnUsage, VlSelectionFields};
use crate::expression::parser::parse;
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormulaTransformSpec {
    pub expr: String,

    #[serde(rename = "as")]
    pub as_: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FormulaTransformSpec {
    fn supported(&self) -> bool {
        if let Ok(expr) = parse(&self.expr) {
            expr.is_supported()
        } else {
            false
        }
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let expr = parse(&self.expr)?;
        Ok(expr.input_vars())
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Ok(parsed) = parse(&self.expr) {
            let usage = parsed.datasets_column_usage(
                datum_var,
                usage_scope,
                task_scope,
                vl_selection_fields,
            );
            TransformColumns::PassThrough {
                usage,
                produced: ColumnUsage::empty().with_column(&self.as_),
            }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns as formula passes through all input columns
        let mut output_local_datetime_columns = Vec::from(input_local_datetime_columns);

        // Try to determine whether formula expression will generate a local datetime column.
        // We don't have full type info so this isn't exact, but we can capture the most common
        // local datetime expressions.
        if self.expr.starts_with("toDate") || self.expr.starts_with("datetime") {
            output_local_datetime_columns.push(self.as_.clone());
        }

        output_local_datetime_columns
    }
}
