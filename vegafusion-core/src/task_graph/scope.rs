/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::supported::BUILT_IN_SIGNALS;
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct TaskScope {
    pub signals: HashSet<String>,
    pub data: HashSet<String>,
    pub scales: HashSet<String>,
    /// Tasks that have definition output variables (e.g. task that produces a signal)
    pub output_var_defs: HashMap<Variable, Variable>,
    pub children: Vec<TaskScope>,
}

impl TaskScope {
    pub fn new() -> Self {
        Self {
            signals: Default::default(),
            data: Default::default(),
            output_var_defs: Default::default(),
            scales: Default::default(),
            children: Default::default(),
        }
    }

    pub fn get_child(&self, scope: &[u32]) -> Result<&TaskScope> {
        let mut child = self;
        for index in scope {
            child = child
                .children
                .get(*index as usize)
                .with_context(|| format!("No group with scope {:?} found", scope))?;
        }
        Ok(child)
    }

    pub fn get_child_mut(&mut self, scope: &[u32]) -> Result<&mut TaskScope> {
        let mut child = self;
        for index in scope {
            child = child
                .children
                .get_mut(*index as usize)
                .with_context(|| format!("No group with scope {:?} found", scope))?;
        }
        Ok(child)
    }

    pub fn add_variable(&mut self, variable: &Variable, scope: &[u32]) -> Result<()> {
        let child = self.get_child_mut(scope)?;

        match variable.ns() {
            VariableNamespace::Signal => {
                child.signals.insert(variable.name.clone());
            }
            VariableNamespace::Data => {
                child.data.insert(variable.name.clone());
            }
            VariableNamespace::Scale => {
                child.scales.insert(variable.name.clone());
            }
        }

        Ok(())
    }

    pub fn add_data_signal(&mut self, data: &str, signal: &str, scope: &[u32]) -> Result<()> {
        let child = self.get_child_mut(scope)?;
        child
            .output_var_defs
            .insert(Variable::new_signal(signal), Variable::new_data(data));
        Ok(())
    }

    pub fn remove_data_signal(&mut self, signal: &str, scope: &[u32]) -> Result<Variable> {
        let child = self.get_child_mut(scope)?;
        child
            .output_var_defs
            .remove(&Variable::new_signal(signal))
            .with_context(|| format!("No data signal named: {}", signal))
    }

    pub fn resolve_scope(&self, variable: &Variable, usage_scope: &[u32]) -> Result<Resolved> {
        // Search for matching variable, start with full usage scope, then iterate up
        for level in (0..=usage_scope.len()).rev() {
            let curr_scope = &usage_scope[0..level];
            let task_scope = self.get_child(curr_scope)?;

            let found_it = match variable.ns() {
                VariableNamespace::Signal => task_scope.signals.contains(&variable.name),
                VariableNamespace::Data => task_scope.data.contains(&variable.name),
                VariableNamespace::Scale => task_scope.scales.contains(&variable.name),
            };
            if found_it {
                // Found it in the regular signal/data/scale
                return Ok(Resolved {
                    var: variable.clone(),
                    scope: Vec::from(curr_scope),
                    output_var: None,
                });
            }

            // Check for output variable
            if let Some(main_var) = task_scope.output_var_defs.get(variable) {
                return Ok(Resolved {
                    var: main_var.clone(),
                    scope: Vec::from(curr_scope),
                    output_var: Some(variable.clone()),
                });
            }

            // Check for built-in signal
            if matches!(variable.ns(), VariableNamespace::Signal)
                && BUILT_IN_SIGNALS.contains(variable.name.as_str())
            {
                return Ok(Resolved {
                    var: variable.clone(),
                    scope: Vec::new(),
                    output_var: None,
                });
            }
        }

        // Didn't find it
        Err(VegaFusionError::internal(&format!(
            "Failed to resolve variable {:?} used in scope {:?}",
            variable, usage_scope
        )))
    }
}

pub struct Resolved {
    pub var: Variable,
    pub scope: Vec<u32>,
    pub output_var: Option<Variable>,
}
