/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::planning::dependency_graph::{get_supported_data_variables, scoped_var_for_input_var};
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};
use crate::spec::mark::MarkSpec;

use crate::spec::signal::SignalSpec;
use crate::task_graph::scope::TaskScope;

use crate::task_graph::graph::ScopedVariable;

use crate::planning::plan::{PlannerConfig, PlannerWarnings};
use std::collections::{HashMap, HashSet};

pub fn extract_server_data(
    client_spec: &mut ChartSpec,
    task_scope: &mut TaskScope,
    warnings: &mut Vec<PlannerWarnings>,
    config: &PlannerConfig,
) -> Result<ChartSpec> {
    let supported_vars = get_supported_data_variables(client_spec, config)?;

    // Check for partially or unsupported data variables
    for (var, dep) in supported_vars.iter() {
        if matches!(var.0.ns(), VariableNamespace::Data)
            && !matches!(dep, DependencyNodeSupported::Supported)
        {
            let message = if var.1.is_empty() {
                format!(
                    "Some transforms applied to the '{}' dataset are not yet supported",
                    var.0.name
                )
            } else {
                format!("Some transforms applied to the '{}' dataset with scope {:?} are not yet supported", var.0.name, var.1.as_slice())
            };
            warnings.push(PlannerWarnings::UnsupportedTransforms(message));
        }
    }

    let mut extract_server_visitor =
        ExtractServerDependenciesVisitor::new(supported_vars, task_scope);
    client_spec.walk_mut(&mut extract_server_visitor)?;

    Ok(extract_server_visitor.server_spec)
}

#[derive(Debug)]
pub struct ExtractServerDependenciesVisitor<'a> {
    pub server_spec: ChartSpec,
    supported_vars: HashMap<ScopedVariable, DependencyNodeSupported>,
    task_scope: &'a mut TaskScope,
}

impl<'a> ExtractServerDependenciesVisitor<'a> {
    pub fn new(
        supported_vars: HashMap<ScopedVariable, DependencyNodeSupported>,
        task_scope: &'a mut TaskScope,
    ) -> Self {
        let server_spec: ChartSpec = ChartSpec {
            schema: "https://vega.github.io/schema/vega/v5.json".into(),
            ..Default::default()
        };
        Self {
            server_spec,
            supported_vars,
            task_scope,
        }
    }
}

impl<'a> MutChartVisitor for ExtractServerDependenciesVisitor<'a> {
    /// Extract data definitions, splitting partially supported transform pipelines
    fn visit_data(&mut self, data: &mut DataSpec, scope: &[u32]) -> Result<()> {
        let data_var: ScopedVariable = (Variable::new_data(&data.name), Vec::from(scope));
        match self.supported_vars.get(&data_var) {
            Some(DependencyNodeSupported::PartiallySupported) => {
                // Split transforms at first unsupported transform.
                // Note: There could be supported transforms in the client_tx after an unsupported
                // transform.

                // Count the number of leading supported transforms. These are transforms that
                // are themselves supported with all supported input dependencies
                let mut pipeline_vars = HashSet::new();
                let mut num_supported = 0;
                'outer: for (i, tx) in data.transform.iter().enumerate() {
                    if tx.supported() {
                        if let Ok(input_vars) = tx.input_vars() {
                            for input_var in input_vars {
                                if let Ok(scoped_source_var) =
                                    scoped_var_for_input_var(&input_var, scope, self.task_scope)
                                {
                                    if !pipeline_vars.contains(&scoped_source_var)
                                        && !self.supported_vars.contains_key(&scoped_source_var)
                                    {
                                        // Dependency is not supported and it was not produced earlier in the transform pipeline
                                        break 'outer;
                                    }
                                } else {
                                    // Failed to get input vars for transform (e.g. expression parse failure)
                                    break 'outer;
                                }
                            }
                        }
                        // Add output signals so we know they are available later
                        for sig in &tx.output_signals() {
                            pipeline_vars.insert((Variable::new_signal(sig), Vec::from(scope)));
                        }
                    } else {
                        // Full transform not supported
                        break 'outer;
                    }
                    num_supported = i + 1;
                }

                let server_tx: Vec<_> = Vec::from(&data.transform[..num_supported]);
                let client_tx: Vec<_> = Vec::from(&data.transform[num_supported..]);

                // Compute new name for server data
                let mut server_name = data.name.clone();
                server_name.insert_str(0, "_server_");

                // Clone data for use on server (with updated name)
                let mut server_data = data.clone();
                server_data.name = server_name.clone();
                server_data.transform = server_tx;

                let server_signals = server_data.output_signals();
                // Update server spec
                if scope.is_empty() {
                    self.server_spec.data.push(server_data)
                } else {
                    let server_group = self.server_spec.get_nested_group_mut(scope)?;
                    server_group.data.push(server_data);
                }

                // Update client data spec:
                //   - Same name
                //   - Add source of server
                //   - Update remaining transforms
                data.source = Some(server_name.clone());
                data.format = None;
                data.values = None;
                data.transform = client_tx;
                data.on = None;
                data.url = None;

                // Update scope
                //  - Add new data variable to task scope
                self.task_scope
                    .add_variable(&Variable::new_data(&server_name), scope)?;

                // - Handle signals generated by transforms that have been moved to the server spec
                for sig in &server_signals {
                    self.task_scope.remove_data_signal(sig, scope)?;
                    self.task_scope.add_data_signal(&server_name, sig, scope)?;
                }
            }
            Some(DependencyNodeSupported::Supported) => {
                // Add clone of full server data
                let server_data = data.clone();
                if scope.is_empty() {
                    self.server_spec.data.push(server_data)
                } else {
                    let server_group = self.server_spec.get_nested_group_mut(scope)?;
                    server_group.data.push(server_data);
                }

                // Clear everything except name from client spec
                data.format = None;
                data.source = None;
                data.values = None;
                data.transform = Vec::new();
                data.on = None;
                data.url = None;
            }
            _ => {
                // Nothing to do
            }
        }

        Ok(())
    }

    fn visit_signal(&mut self, signal: &mut SignalSpec, scope: &[u32]) -> Result<()> {
        // check if signal is supported
        let scoped_signal_var = (Variable::new_signal(&signal.name), Vec::from(scope));
        if self.supported_vars.contains_key(&scoped_signal_var) {
            // Move signal to server
            let server_signal = signal.clone();
            if scope.is_empty() {
                self.server_spec.signals.push(server_signal)
            } else {
                let server_group = self.server_spec.get_nested_group_mut(scope)?;
                server_group.signals.push(server_signal);
            }

            // What should be cleared from client signal?
            // signal.init = None;
            // signal.update = None;
            // signal.on = Default::default();
        }
        Ok(())
    }

    fn visit_group_mark(&mut self, _mark: &mut MarkSpec, scope: &[u32]) -> Result<()> {
        // Initialize group mark in server spec
        let parent_scope = &scope[..scope.len() - 1];
        let new_group = MarkSpec {
            type_: "group".to_string(),
            name: None,
            from: None,
            sort: None,
            encode: None,
            data: vec![],
            signals: vec![],
            marks: vec![],
            scales: vec![],
            axes: vec![],
            transform: vec![],
            title: None,
            extra: Default::default(),
        };
        if parent_scope.is_empty() {
            self.server_spec.marks.push(new_group);
        } else {
            let parent_group = self.server_spec.get_nested_group_mut(parent_scope)?;
            parent_group.marks.push(new_group);
        }

        Ok(())
    }
}
