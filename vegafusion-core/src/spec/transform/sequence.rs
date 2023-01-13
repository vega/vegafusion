use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Result;
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::expression::parser::parse;

use crate::spec::values::{NumberOrSignalSpec, SignalExpressionSpec};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the sequence transform.
/// This is currently only needed to report the proper input dependencies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SequenceTransformSpec {
    pub start: NumberOrSignalSpec,
    pub stop: NumberOrSignalSpec,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<NumberOrSignalSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl SequenceTransformSpec {
    pub fn as_(&self) -> String {
        self.as_.clone().unwrap_or_else(|| "data".to_string())
    }
}

impl TransformSpecTrait for SequenceTransformSpec {
    fn supported(&self) -> bool {
        let mut signals = vec![&self.start, &self.stop];
        if let Some(step) = &self.step {
            signals.push(step);
        }
        for sig in signals {
            if let NumberOrSignalSpec::Signal(SignalExpressionSpec { signal }) = sig {
                if let Ok(expr) = parse(signal.as_str()) {
                    if !expr.is_supported() {
                        // Signal expression is not supported
                        return false;
                    }
                } else {
                    // Failed to parse expression, not supported
                    return false;
                }
            }
        }
        true
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let mut input_vars: Vec<InputVariable> = Vec::new();
        input_vars.extend(self.start.input_vars()?);
        input_vars.extend(self.stop.input_vars()?);
        if let Some(step) = &self.step {
            input_vars.extend(step.input_vars()?);
        }

        Ok(input_vars)
    }

    fn transform_columns(
        &self,
        _datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        // Sequence uses not columns and produces a single column
        TransformColumns::Overwrite {
            usage: DatasetsColumnUsage::empty(),
            produced: ColumnUsage::from(self.as_().as_str()),
        }
    }
}
