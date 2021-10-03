use crate::expression::compiler::call::{default_callables, VeagFusionCallable};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;

#[derive(Clone)]
pub struct CompilationConfig {
    pub signal_scope: HashMap<String, ScalarValue>,
    pub callable_scope: HashMap<String, VeagFusionCallable>,
}

impl Default for CompilationConfig {
    fn default() -> Self {
        Self {
            signal_scope: Default::default(),
            callable_scope: default_callables(),
        }
    }
}
