use crate::proto::gen::tasks::{Task, ScopedVariable, task::TaskKind, TransformsTask, Variable, ScanUrlTask};
use crate::proto::gen::tasks::TaskValue as ProtoTaskValue;
use crate::task_graph::task_value::TaskValue;
use std::convert::TryFrom;
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::transforms::TransformPipeline;
use crate::transform::TransformDependencies;
use std::hash::{Hash, Hasher};
use prost::Message;


impl Task {
    pub fn task_kind(&self) -> &TaskKind {
        self.task_kind.as_ref().unwrap()
    }
    pub fn variable(&self) -> &Variable {
        self.variable.as_ref().unwrap()
    }

    pub fn scope(&self) -> &[u32] {
        self.scope.as_slice()
    }

    pub fn new_value(variable: Variable, scope: &[u32], value: TaskValue) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::Value(ProtoTaskValue::try_from(&value).unwrap()))
        }
    }

    pub fn to_value(&self) -> Result<TaskValue> {
        if let TaskKind::Value(value) = self.task_kind() {
            Ok(TaskValue::try_from(value)?)
        } else {
            Err(VegaFusionError::internal("Task is not a TaskValue"))
        }
    }

    pub fn new_transforms(variable: Variable, scope: &[u32], transforms: TransformsTask) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::Transforms(transforms))
        }
    }

    pub fn as_transforms(&self) -> Result<&TransformsTask> {
        if let TaskKind::Transforms(transforms) = self.task_kind() {
            Ok(transforms)
        } else {
            Err(VegaFusionError::internal("Task is not a TransformTask"))
        }
    }

    pub fn new_scan_url(variable: Variable, scope: &[u32], task: ScanUrlTask) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::Url(task))
        }
    }

    pub fn input_vars(&self) -> Vec<Variable> {
        match self.task_kind() {
            TaskKind::Value(_) => Vec::new(),
            TaskKind::Url(task) => task.input_vars(),
            TaskKind::Transforms(task) => task.input_vars(),
        }
    }

    pub fn output_signals(&self) -> Vec<String> {
        match self.task_kind() {
            TaskKind::Value(_) => Vec::new(),
            TaskKind::Url(task) => task.output_signals(),
            TaskKind::Transforms(task) => task.output_signals(),
        }
    }
}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut proto_bytes: Vec<u8> = Vec::with_capacity(self.encoded_len());

        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        self.encode(&mut proto_bytes).unwrap();
        proto_bytes.hash(state);
    }
}

pub trait TaskDependencies {
    fn input_vars(&self) -> Vec<Variable> { Vec::new() }
    fn output_signals(&self) -> Vec<String> { Vec::new() }
}