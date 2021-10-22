use crate::proto::gen::tasks::{TransformsTask, Variable};
use crate::task_graph::task::TaskDependencies;
use crate::transform::TransformDependencies;

impl TaskDependencies for TransformsTask {
    fn input_vars(&self) -> Vec<Variable> {
        // Make sure source dataset is the first input variable
        let mut input_vars = vec![Variable::new_data(&self.source)];
        input_vars.extend(self.pipeline.as_ref().unwrap().input_vars());
        input_vars
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline.as_ref().unwrap().output_vars()
    }
}
