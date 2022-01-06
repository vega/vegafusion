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
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::proto::gen::tasks::Task;
use crate::spec::data::DataSpec;
use crate::spec::mark::MarkSpec;
use crate::spec::scale::ScaleSpec;
use crate::spec::signal::SignalSpec;
use crate::spec::visitors::{
    DefinitionVarsChartVisitor, InputVarsChartVisitor, MakeTaskScopeVisitor, MakeTasksVisitor,
    UpdateVarsChartVisitor,
};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use itertools::sorted;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChartSpec {
    #[serde(rename = "$schema", default = "default_schema")]
    pub schema: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<DataSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<SignalSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub marks: Vec<MarkSpec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scales: Vec<ScaleSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

pub fn default_schema() -> String {
    String::from("https://vega.github.io/schema/vega/v5.json")
}

impl ChartSpec {
    pub fn walk(&self, visitor: &mut dyn ChartVisitor) -> Result<()> {
        // Top-level with empty scope
        let scope: Vec<u32> = Vec::new();
        for data in &self.data {
            visitor.visit_data(data, &scope)?;
        }
        for scale in &self.scales {
            visitor.visit_scale(scale, &scope)?;
        }
        for signal in &self.signals {
            visitor.visit_signal(signal, &scope)?;
        }

        // Child groups
        let mut group_index = 0;
        for mark in &self.marks {
            if mark.type_ == "group" {
                // Add group index to scope
                let mut nested_scope = scope.clone();
                nested_scope.push(group_index);

                visitor.visit_group_mark(mark, &nested_scope)?;
                mark.walk(visitor, &nested_scope)?;
                group_index += 1;
            } else {
                // Keep parent scope
                visitor.visit_non_group_mark(mark, &scope)?;
            }
        }

        Ok(())
    }

    pub fn walk_mut(&mut self, visitor: &mut dyn MutChartVisitor) -> Result<()> {
        // Top-level with empty scope
        let scope: Vec<u32> = Vec::new();
        for data in &mut self.data {
            visitor.visit_data(data, &scope)?;
        }
        for scale in &mut self.scales {
            visitor.visit_scale(scale, &scope)?;
        }
        for signal in &mut self.signals {
            visitor.visit_signal(signal, &scope)?;
        }

        // Child groups
        let mut group_index = 0;
        for mark in &mut self.marks {
            if mark.type_ == "group" {
                // Add group index to scope
                let mut nested_scope = scope.clone();
                nested_scope.push(group_index);

                visitor.visit_group_mark(mark, &nested_scope)?;
                mark.walk_mut(visitor, &nested_scope)?;
                group_index += 1;
            } else {
                // Keep parent scope
                visitor.visit_non_group_mark(mark, &scope)?;
            }
        }

        Ok(())
    }

    pub fn to_task_scope(&self) -> Result<TaskScope> {
        let mut visitor = MakeTaskScopeVisitor::new();
        self.walk(&mut visitor)?;
        Ok(visitor.task_scope)
    }

    pub fn to_tasks(&self) -> Result<Vec<Task>> {
        let mut visitor = MakeTasksVisitor::new();
        self.walk(&mut visitor)?;
        Ok(visitor.tasks)
    }

    pub fn get_group(&self, group_index: u32) -> Result<&MarkSpec> {
        self.marks
            .iter()
            .filter(|m| m.type_ == "group")
            .nth(group_index as usize)
            .with_context(|| format!("No group with index {}", group_index))
    }

    pub fn get_nested_group(&self, path: &[u32]) -> Result<&MarkSpec> {
        if path.is_empty() {
            return Err(VegaFusionError::internal(
                "Nested group scope may not be empty",
            ));
        }
        let mut group = self.get_group(path[0])?;
        for group_index in &path[1..] {
            group = group.get_group(*group_index)?;
        }
        Ok(group)
    }

    pub fn get_group_mut(&mut self, group_index: u32) -> Result<&mut MarkSpec> {
        self.marks
            .iter_mut()
            .filter(|m| m.type_ == "group")
            .nth(group_index as usize)
            .with_context(|| format!("No group with index {}", group_index))
    }

    pub fn get_nested_group_mut(&mut self, path: &[u32]) -> Result<&mut MarkSpec> {
        if path.is_empty() {
            return Err(VegaFusionError::internal("Path may not be empty"));
        }
        let mut group = self.get_group_mut(path[0])?;
        for group_index in &path[1..] {
            group = group.get_group_mut(*group_index)?;
        }
        Ok(group)
    }

    pub fn get_nested_signal(&self, path: &[u32], name: &str) -> Result<&SignalSpec> {
        let signals = if path.is_empty() {
            &self.signals
        } else {
            let group = self.get_nested_group(path)?;
            &group.signals
        };
        signals
            .iter()
            .find(|s| s.name == name)
            .with_context(|| format!("No signal named {} found at path {:?}", name, path))
    }

    pub fn get_nested_signal_mut(&mut self, path: &[u32], name: &str) -> Result<&mut SignalSpec> {
        let signals = if path.is_empty() {
            &mut self.signals
        } else {
            let group = self.get_nested_group_mut(path)?;
            &mut group.signals
        };
        signals
            .iter_mut()
            .find(|s| s.name == name)
            .with_context(|| format!("No signal named {} found at path {:?}", name, path))
    }

    pub fn get_nested_data(&self, path: &[u32], name: &str) -> Result<&DataSpec> {
        let signals = if path.is_empty() {
            &self.data
        } else {
            let group = self.get_nested_group(path)?;
            &group.data
        };
        signals
            .iter()
            .find(|s| s.name == name)
            .with_context(|| format!("No data named {} found at path {:?}", name, path))
    }

    pub fn get_nested_data_mut(&mut self, path: &[u32], name: &str) -> Result<&mut DataSpec> {
        let signals = if path.is_empty() {
            &mut self.data
        } else {
            let group = self.get_nested_group_mut(path)?;
            &mut group.data
        };
        signals
            .iter_mut()
            .find(|s| s.name == name)
            .with_context(|| format!("No data named {} found at path {:?}", name, path))
    }

    pub fn add_nested_signal(
        &mut self,
        path: &[u32],
        spec: SignalSpec,
        index: Option<usize>,
    ) -> Result<()> {
        let signals = if path.is_empty() {
            &mut self.signals
        } else {
            let group = self.get_nested_group_mut(path)?;
            &mut group.signals
        };
        match index {
            Some(index) => {
                signals.insert(index, spec);
            }
            None => {
                signals.push(spec);
            }
        }
        Ok(())
    }

    pub fn add_nested_data(
        &mut self,
        path: &[u32],
        spec: DataSpec,
        index: Option<usize>,
    ) -> Result<()> {
        let data = if path.is_empty() {
            &mut self.data
        } else {
            let group = self.get_nested_group_mut(path)?;
            &mut group.data
        };
        match index {
            Some(index) => {
                data.insert(index, spec);
            }
            None => {
                data.push(spec);
            }
        }
        Ok(())
    }

    pub fn definition_vars(&self) -> Result<Vec<ScopedVariable>> {
        let mut visitor = DefinitionVarsChartVisitor::new();
        self.walk(&mut visitor)?;
        Ok(sorted(visitor.definition_vars).collect())
    }

    pub fn update_vars(&self, task_scope: &TaskScope) -> Result<Vec<ScopedVariable>> {
        let mut visitor = UpdateVarsChartVisitor::new(task_scope);
        self.walk(&mut visitor)?;
        Ok(sorted(visitor.update_vars).collect())
    }

    pub fn input_vars(&self, task_scope: &TaskScope) -> Result<Vec<ScopedVariable>> {
        let mut visitor = InputVarsChartVisitor::new(task_scope);
        self.walk(&mut visitor)?;
        Ok(sorted(visitor.input_vars).collect())
    }
}

pub trait ChartVisitor {
    fn visit_data(&mut self, _data: &DataSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_signal(&mut self, _signal: &SignalSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_scale(&mut self, _scale: &ScaleSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_non_group_mark(&mut self, _mark: &MarkSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_group_mark(&mut self, _mark: &MarkSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
}

pub trait MutChartVisitor {
    fn visit_data(&mut self, _data: &mut DataSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_signal(&mut self, _signal: &mut SignalSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_scale(&mut self, _scale: &mut ScaleSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_non_group_mark(&mut self, _mark: &mut MarkSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
    fn visit_group_mark(&mut self, _mark: &mut MarkSpec, _scope: &[u32]) -> Result<()> {
        Ok(())
    }
}
