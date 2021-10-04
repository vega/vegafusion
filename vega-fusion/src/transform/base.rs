use std::fmt::Debug;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use crate::expression::compiler::config::CompilationConfig;
use crate::variable::Variable;
use crate::error::Result;
use datafusion::scalar::ScalarValue;


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
