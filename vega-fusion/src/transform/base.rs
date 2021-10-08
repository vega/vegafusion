use crate::error::Result;
use crate::expression::compiler::config::CompilationConfig;
use crate::variable::Variable;
use datafusion::dataframe::DataFrame;
use datafusion::scalar::ScalarValue;
use std::fmt::Debug;
use std::sync::Arc;

pub trait TransformTrait: Debug + Send + Sync {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)>;

    fn input_vars(&self) -> Vec<Variable> {
        Vec::new()
    }

    fn output_signals(&self) -> Vec<String> {
        Vec::new()
    }
}
