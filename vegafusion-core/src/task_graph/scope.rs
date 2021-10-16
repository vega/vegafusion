use std::collections::{HashSet, HashMap};
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::proto::gen::tasks::{Variable, VariableNamespace};

#[derive(Clone, Debug, Default)]
pub struct TaskScope {
    pub signals: HashSet<String>,
    pub data: HashSet<String>,
    /// Mapping from signal name to the dataset with the transform that generates the signal
    pub data_signal: HashMap<String, String>,
    pub scales: HashSet<String>,
    pub children: Vec<TaskScope>,
}

impl TaskScope {
    pub fn new() -> Self {
        Self {
            signals: Default::default(),
            data: Default::default(),
            data_signal: Default::default(),
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
        let mut child = self.get_child_mut(scope)?;

        match variable.namespace() {
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
        let mut child = self.get_child_mut(scope)?;
        child.data_signal.insert(signal.to_string(), data.to_string());
        Ok(())
    }

    pub fn resolve_scope(&self, variable: &Variable, usage_scope: &[u32]) -> Result<Resolved> {
        // Search for matching variable, start with full usage scope, then iterate up
        for level in (0..=usage_scope.len()).rev() {
            let curr_scope = &usage_scope[0..level];
            let task_scope = self.get_child(curr_scope)?;

            let found_it = match variable.namespace() {
                VariableNamespace::Signal => {
                    task_scope.signals.contains(&variable.name)
                }
                VariableNamespace::Data => {
                    task_scope.data.contains(&variable.name)
                }
                VariableNamespace::Scale => {
                    task_scope.scales.contains(&variable.name)
                }
            };
            if found_it {
                // Found it in the regular signal/data/scale
                return Ok(Resolved {
                    var: variable.clone(),
                    scope: Vec::from(curr_scope),
                    signal: None
                })
            }

            // Check for data signal
            if matches!(variable.namespace(), VariableNamespace::Signal) {
                if let Some(data) = task_scope.data_signal.get(&variable.name) {
                    return Ok(Resolved {
                        var: Variable::new_data(data),
                        scope: Vec::from(curr_scope),
                        signal: Some(variable.name.clone())
                    })
                }
            }
        }

        // Didn't find it
        Err(VegaFusionError::internal(&format!(
            "Failed to resolve variable {:?} used in scope {:?}", variable, usage_scope
        )))
    }
}

pub struct Resolved {
    pub var: Variable,
    pub scope: Vec<u32>,
    pub signal: Option<String>,
}
