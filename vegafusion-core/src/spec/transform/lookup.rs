use crate::spec::transform::TransformSpecTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Result;

use crate::proto::gen::tasks::Variable;
use crate::task_graph::task::InputVariable;

/// Struct that serializes to Vega spec for the lookup transform.
/// This is currently only needed to report the proper input dependencies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LookupTransformSpec {
    pub from: String,

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
}
