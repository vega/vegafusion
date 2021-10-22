use crate::proto::gen::tasks::{ScanUrlTask, Variable};
use crate::task_graph::task::{TaskDependencies, InputVariable};

impl ScanUrlTask {
    pub fn url(&self) -> &Variable {
        self.url.as_ref().unwrap()
    }
}

impl TaskDependencies for ScanUrlTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        vec![InputVariable{
            var: self.url().clone(),
            propagate: true,
        }]
    }
}