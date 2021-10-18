use crate::proto::gen::tasks::{ScanUrlTask, Variable};
use crate::task_graph::task::TaskDependencies;

impl ScanUrlTask {
    pub fn url(&self) -> &Variable {
        self.url.as_ref().unwrap()
    }
}

impl TaskDependencies for ScanUrlTask {
    fn input_vars(&self) -> Vec<Variable> {
        vec![self.url().clone()]
    }
}