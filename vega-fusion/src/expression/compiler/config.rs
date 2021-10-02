use datafusion::scalar::ScalarValue;
use std::collections::HashMap;

#[derive(Clone)]
pub struct CompilationConfig {
    pub signal_scope: HashMap<String, ScalarValue>,
}

impl Default for CompilationConfig {
    fn default() -> Self {
        Self {
            signal_scope: Default::default(),
        }
    }
}
