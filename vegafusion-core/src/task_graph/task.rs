/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::tasks::{
    task::TaskKind, DataSourceTask, DataUrlTask, DataValuesTask, NodeValueIndex, Task, Variable,
};
use crate::proto::gen::tasks::{SignalTask, TaskValue as ProtoTaskValue};
use crate::task_graph::task_value::TaskValue;
use std::convert::TryFrom;

use crate::proto::gen::expression::Expression;
use prost::Message;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InputVariable {
    pub var: Variable,
    pub propagate: bool,
}

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
            task_kind: Some(TaskKind::Value(ProtoTaskValue::try_from(&value).unwrap())),
        }
    }

    pub fn to_value(&self) -> Result<TaskValue> {
        if let TaskKind::Value(value) = self.task_kind() {
            Ok(TaskValue::try_from(value)?)
        } else {
            Err(VegaFusionError::internal("Task is not a TaskValue"))
        }
    }

    pub fn new_data_url(variable: Variable, scope: &[u32], task: DataUrlTask) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::DataUrl(task)),
        }
    }

    pub fn new_data_values(variable: Variable, scope: &[u32], task: DataValuesTask) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::DataValues(task)),
        }
    }

    pub fn new_data_source(variable: Variable, scope: &[u32], task: DataSourceTask) -> Self {
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(TaskKind::DataSource(task)),
        }
    }

    pub fn new_signal(variable: Variable, scope: &[u32], expr: Expression) -> Self {
        let task_kind = TaskKind::Signal(SignalTask { expr: Some(expr) });
        Self {
            variable: Some(variable),
            scope: Vec::from(scope),
            task_kind: Some(task_kind),
        }
    }

    pub fn input_vars(&self) -> Vec<InputVariable> {
        match self.task_kind() {
            TaskKind::Value(_) => Vec::new(),
            TaskKind::DataUrl(task) => task.input_vars(),
            TaskKind::DataSource(task) => task.input_vars(),
            TaskKind::DataValues(task) => task.input_vars(),
            TaskKind::Signal(task) => {
                let expr = task.expr.as_ref().unwrap();
                expr.input_vars()
            }
        }
    }

    pub fn output_vars(&self) -> Vec<Variable> {
        match self.task_kind() {
            TaskKind::Value(_) => Vec::new(),
            TaskKind::DataUrl(task) => task.output_vars(),
            TaskKind::DataSource(task) => task.output_vars(),
            TaskKind::DataValues(task) => task.output_vars(),
            TaskKind::Signal(_) => Vec::new(),
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut proto_bytes: Vec<u8> = Vec::with_capacity(self.encoded_len());

        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        self.encode(&mut proto_bytes).unwrap();
        proto_bytes.hash(state);
    }
}

pub trait TaskDependencies {
    fn input_vars(&self) -> Vec<InputVariable> {
        Vec::new()
    }
    fn output_vars(&self) -> Vec<Variable> {
        Vec::new()
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for NodeValueIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_index.hash(state);
        self.output_index.hash(state);
    }
}

impl Eq for NodeValueIndex {}
