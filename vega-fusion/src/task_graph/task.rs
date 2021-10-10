use datafusion::scalar::ScalarValue;
use crate::data::table::VegaFusionTable;
use crate::variable::{ScopedVariable, Variable};
use crate::error::Result;
use crate::transform::pipeline::TransformPipeline;
use crate::expression::compiler::config::CompilationConfig;

pub enum TaskValue {
    Scalar(ScalarValue),
    Table(VegaFusionTable),
}


pub enum Task {
    Value,
    AsyncTaskFn {
        function: Box<AsyncTaskFn>,
        inputs: Vec<ScopedVariable>,
        outputs: Vec<ScopedVariable>
    }
}

impl Task {
    pub fn from_transforms(pipeline: &TransformPipeline) -> Self {
        // TODO Use scope info to create ScopedVariables from the unscoped inputs and outputs
        //      returned from pipeline
        let inputs: Vec<_> = pipeline.input_vars().iter().map(
            |v| ScopedVariable::new(v.namespace.clone(), &v.name, Vec::new())
        ).collect();

        let outputs: Vec<_> = pipeline.output_signals().iter().map(
            |s| ScopedVariable::new_signal(s.as_str(), Vec::new())
        ).collect();

        Self::AsyncTaskFn {
            function: Box::new(AsyncTaskFn::Transform(pipeline.clone())),
            inputs,
            outputs
        }
    }
}


pub enum AsyncTaskFn {
     ScanUrl,
     Transform(TransformPipeline),
}

impl AsyncTaskFn {
    fn inputs(&self) -> Vec<Variable> {
        match self {
            AsyncTaskFn::ScanUrl => { todo!() }
            AsyncTaskFn::Transform(pipeline) => {
                pipeline.input_vars()
            }
        }
    }

    fn output_signals(&self) -> Vec<String> {
        match self {
            AsyncTaskFn::ScanUrl => { todo!() }
            AsyncTaskFn::Transform(pipeline) => {
                pipeline.output_signals()
            }
        }
    }

    async fn eval(&self, inputs: &[TaskValue]) -> Result<(TaskValue, Vec<ScalarValue>)> {
        match self {
            AsyncTaskFn::ScanUrl => { todo!() }
            AsyncTaskFn::Transform(pipeline) => {
                let df = if let TaskValue::Table(table) = &inputs[0] {
                    table.to_dataframe()?
                } else {
                    unreachable!("Expected Table")
                };

                // Remaining inputs added to compilation config
                let mut config = CompilationConfig::default();
                for (value, var) in inputs[1..].iter().zip(self.inputs()) {
                    match value {
                        TaskValue::Scalar(value) => {
                            config.signal_scope.insert(var.name.clone(), value.clone());
                        }
                        TaskValue::Table(value) => {
                            config.data_scope.insert(var.name.clone(), value.clone());
                        }
                    }
                }

                let (result_df, result_outputs) = pipeline.eval(df, &config)?;
                let result_table = TaskValue::Table(VegaFusionTable::from_dataframe_async(&result_df).await?);
                Ok((result_table, result_outputs))
            }
        }
    }
}
