/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::parser::parse;
use crate::expression::supported::BUILT_IN_SIGNALS;
use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};
use crate::spec::mark::MarkSpec;
use crate::spec::scale::ScaleSpec;
use crate::spec::signal::SignalSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use petgraph::algo::toposort;
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use petgraph::Incoming;
use std::collections::{HashMap, HashSet};

/// get HashSet of all data variables with fully supported parents that are themselves fully or
/// partially supported
pub fn get_supported_data_variables(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
) -> Result<HashMap<ScopedVariable, DependencyNodeSupported>> {
    let data_graph = build_dependency_graph(chart_spec, config)?;
    // Sort dataset nodes topologically
    let nodes: Vec<NodeIndex> = match toposort(&data_graph, None) {
        Ok(v) => v,
        Err(err) => {
            return Err(VegaFusionError::internal(format!(
                "Failed to sort datasets topologically: {:?}",
                err
            )))
        }
    };

    // Traverse nodes and save those to supported_vars that are supported with all supported
    // parents
    let mut all_supported_vars = HashMap::new();

    for node_index in &nodes {
        let (scoped_var, node_supported) = data_graph.node_weight(*node_index).unwrap();

        // Unsupported nodes not included
        if !matches!(node_supported, DependencyNodeSupported::Unsupported) {
            // Check whether all parents are fully supported
            let all_parents_supported = data_graph
                .edges_directed(*node_index, Incoming)
                .into_iter()
                .map(|edge| data_graph.node_weight(edge.source()).unwrap().0.clone())
                .all(|parent_var| {
                    matches!(
                        all_supported_vars.get(&parent_var),
                        Some(DependencyNodeSupported::Supported)
                    )
                });

            if all_parents_supported {
                all_supported_vars.insert(scoped_var.clone(), node_supported.clone());
            } else {
                // Check if all input Datasets are supported. e.g. A data node with supported source
                // but some unsupported transform dependencies
                let all_dataset_inputs_supported = data_graph
                    .edges_directed(*node_index, Incoming)
                    .into_iter()
                    .map(|edge| data_graph.node_weight(edge.source()).unwrap().0.clone())
                    .all(|parent_var| match parent_var.0.namespace() {
                        VariableNamespace::Data => {
                            matches!(
                                all_supported_vars.get(&parent_var),
                                Some(DependencyNodeSupported::Supported)
                            )
                        }
                        _ => true,
                    });

                if all_dataset_inputs_supported {
                    all_supported_vars.insert(
                        scoped_var.clone(),
                        DependencyNodeSupported::PartiallySupported,
                    );
                }
            }
        }
    }

    // Traverse again, this time keep all data nodes, but only keep signals that are ancestors
    // of supported data nodes. This is to avoid bringing over unnecessary signals
    let mut supported_vars = HashMap::new();
    for node_index in &nodes {
        let (scoped_var, _) = data_graph.node_weight(*node_index).unwrap();
        match scoped_var.0.namespace() {
            VariableNamespace::Data => {
                if let Some(supported) = all_supported_vars.get(scoped_var) {
                    // Keep all supported data nodes
                    supported_vars.insert(scoped_var.clone(), supported.clone());
                }
            }
            VariableNamespace::Signal => {
                if all_supported_vars.contains_key(scoped_var) {
                    // Check if any dependent nodes are supported data sets
                    let mut dfs = petgraph::visit::Dfs::new(&data_graph, *node_index);
                    'dfs: while let Some(dfs_node_index) = dfs.next(&data_graph) {
                        let (dfs_scoped_var, _) = data_graph.node_weight(dfs_node_index).unwrap();
                        if matches!(dfs_scoped_var.0.namespace(), VariableNamespace::Data)
                            && all_supported_vars.contains_key(dfs_scoped_var)
                        {
                            // Found supported child data node. Add signal as supported and bail
                            // out of DFS
                            supported_vars.insert(
                                scoped_var.clone(),
                                all_supported_vars.get(dfs_scoped_var).unwrap().clone(),
                            );
                            break 'dfs;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    Ok(supported_vars)
}

pub fn build_dependency_graph(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
) -> Result<DiGraph<(ScopedVariable, DependencyNodeSupported), ()>> {
    // Initialize graph with nodes
    let mut nodes_visitor = AddDependencyNodesVisitor::new(config.extract_inline_data);
    chart_spec.walk(&mut nodes_visitor)?;

    // Add dependency edges
    let task_scope = chart_spec.to_task_scope()?;
    let mut edges_visitor = AddDependencyEdgesVisitor::new(
        &mut nodes_visitor.dependency_graph,
        &nodes_visitor.node_indexes,
        &task_scope,
    );
    chart_spec.walk(&mut edges_visitor)?;

    Ok(nodes_visitor.dependency_graph)
}

/// Visitor to initialize directed graph with nodes for each dataset (no edges yet)
#[derive(Debug, Default)]
pub struct AddDependencyNodesVisitor {
    pub dependency_graph: DiGraph<(ScopedVariable, DependencyNodeSupported), ()>,
    pub node_indexes: HashMap<ScopedVariable, NodeIndex>,
    pub extract_inline_data: bool,
}

impl AddDependencyNodesVisitor {
    pub fn new(extract_inline_data: bool) -> Self {
        let mut dependency_graph = DiGraph::new();
        let mut node_indexes = HashMap::new();

        // Initialize with nodes for all built-in signals (e.g. width, height, etc.)
        for sig in BUILT_IN_SIGNALS.iter() {
            let scoped_var = (Variable::new_signal(sig), Vec::new());
            let node_index = dependency_graph
                .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
            node_indexes.insert(scoped_var, node_index);
        }

        Self {
            dependency_graph,
            node_indexes,
            extract_inline_data,
        }
    }
}

impl ChartVisitor for AddDependencyNodesVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Add scoped variable for dataset as node
        let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));
        let data_suported = data.supported(self.extract_inline_data);
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), data_suported.clone()));
        self.node_indexes.insert(scoped_var, node_index);

        // Add signals defined by transforms
        for tx in data.transform.iter() {
            for sig in tx.output_signals() {
                let scoped_var = (Variable::new_signal(&sig), Vec::from(scope));
                let node_index = self
                    .dependency_graph
                    .add_node((scoped_var.clone(), data_suported.clone()));
                self.node_indexes.insert(scoped_var, node_index);
            }
        }

        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        // Add scoped variable for signal as node
        let scoped_var = (Variable::new_signal(&signal.name), Vec::from(scope));
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), signal.supported()));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let scoped_var = (Variable::new_scale(&scale.name), Vec::from(scope));
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }

    fn visit_non_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Named non-group marks can serve as datasets
        if let Some(name) = &mark.name {
            let scoped_var = (Variable::new_data(name), Vec::from(scope));
            let node_index = self
                .dependency_graph
                .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
            self.node_indexes.insert(scoped_var, node_index);
        }
        Ok(())
    }

    fn visit_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        if let Some(from) = &mark.from {
            if let Some(facet) = &from.facet {
                let scoped_var = (Variable::new_data(&facet.name), Vec::from(scope));
                let node_index = self
                    .dependency_graph
                    .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
                self.node_indexes.insert(scoped_var, node_index);
            }
        }

        // Named group marks can serve as datasets
        if let Some(name) = &mark.name {
            let parent_scope = Vec::from(&scope[..scope.len()]);
            let scoped_var = (Variable::new_data(name), parent_scope);
            let node_index = self
                .dependency_graph
                .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
            self.node_indexes.insert(scoped_var, node_index);
        }

        Ok(())
    }
}

/// Visitor to add directed edges to graph with data nodes
#[derive(Debug)]
pub struct AddDependencyEdgesVisitor<'a> {
    pub dependency_graph: &'a mut DiGraph<(ScopedVariable, DependencyNodeSupported), ()>,
    node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
    task_scope: &'a TaskScope,
}

impl<'a> AddDependencyEdgesVisitor<'a> {
    pub fn new(
        dependency_graph: &'a mut DiGraph<(ScopedVariable, DependencyNodeSupported), ()>,
        node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
        task_scope: &'a TaskScope,
    ) -> Self {
        Self {
            dependency_graph,
            node_indexes,
            task_scope,
        }
    }
}

impl<'a> ChartVisitor for AddDependencyEdgesVisitor<'a> {
    /// Add edges into a data node
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Scoped var for this node
        let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing data node: {:?}", scoped_var))?;

        let mut scoped_source_vars = Vec::new();

        // Get parent node in `source` position
        if let Some(source) = &data.source {
            // Build scoped var for parent node
            let source_var = Variable::new_data(source);
            let resolved = self.task_scope.resolve_scope(&source_var, scope)?;
            let scoped_source_var = (resolved.var, resolved.scope);
            scoped_source_vars.push(scoped_source_var);
        }

        // Get parent nodes for transforms
        let mut output_signals = HashSet::new();
        for tx in &data.transform {
            output_signals.extend(
                tx.output_signals()
                    .iter()
                    .map(|name| (Variable::new_signal(name), Vec::from(scope))),
            );

            for input_var in tx.input_vars()? {
                let scoped_source_var =
                    scoped_var_for_input_var(&input_var, scope, self.task_scope)?;
                // Add edge if dependency is not a signal produced by an earlier transform in the same
                // pipeline
                if !output_signals.contains(&scoped_source_var) {
                    scoped_source_vars.push(scoped_source_var);
                }
            }
        }

        // Add edges from parents to this data node
        for scoped_source_var in scoped_source_vars {
            let source_node_index = self
                .node_indexes
                .get(&scoped_source_var)
                .with_context(|| format!("Missing data node: {:?}", scoped_source_var))?;

            // Add directed edge
            self.dependency_graph
                .add_edge(*source_node_index, *node_index, ());
        }

        Ok(())
    }

    /// Add edges into a signal node
    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        // Scoped var for this node
        let scoped_var = (Variable::new_signal(&signal.name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing signal node: {:?}", scoped_var))?;

        let mut input_vars: Vec<InputVariable> = Vec::new();

        if let Some(update) = &signal.update {
            let expression = parse(update)?;
            // Add edges from nodes to signal (Scale dependencies not supported on server yet)
            input_vars.extend(
                expression
                    .input_vars()
                    .into_iter()
                    .filter(|v| v.var.namespace() != VariableNamespace::Scale),
            );
        }

        if let Some(init) = &signal.init {
            let expression = parse(init)?;
            input_vars.extend(expression.input_vars());
        }

        for input_var in input_vars {
            let scoped_source_var = scoped_var_for_input_var(&input_var, scope, self.task_scope)?;
            let source_node_index = self
                .node_indexes
                .get(&scoped_source_var)
                .with_context(|| format!("Missing data node: {:?}", scoped_source_var))?;

            // Add directed edge
            self.dependency_graph
                .add_edge(*source_node_index, *node_index, ());
        }
        Ok(())
    }
}

pub fn scoped_var_for_input_var(
    input_var: &InputVariable,
    usage_scope: &[u32],
    task_scope: &TaskScope,
) -> Result<ScopedVariable> {
    let source_var = input_var.var.clone();
    let resolved = task_scope.resolve_scope(&source_var, usage_scope)?;
    if let Some(output_var) = resolved.output_var {
        Ok((output_var, resolved.scope))
    } else {
        Ok((resolved.var, resolved.scope))
    }
}
