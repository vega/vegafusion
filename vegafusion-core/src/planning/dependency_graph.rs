use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::parser::parse;
use crate::expression::supported::BUILT_IN_SIGNALS;
use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};
use crate::spec::mark::MarkSpec;
use crate::spec::projection::ProjectionSpec;
use crate::spec::scale::ScaleSpec;
use crate::spec::signal::SignalSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use petgraph::algo::toposort;
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use petgraph::Incoming;
use std::collections::{HashMap, HashSet};

pub type DependencyGraph = DiGraph<(ScopedVariable, DependencyNodeSupported), ()>;

pub fn toposort_dependency_graph(data_graph: &DependencyGraph) -> Result<Vec<NodeIndex>> {
    Ok(match toposort(&data_graph, None) {
        Ok(v) => v,
        Err(err) => {
            return Err(VegaFusionError::internal(format!(
                "Failed to sort datasets topologically: {err:?}"
            )))
        }
    })
}

/// get HashSet of all data variables with fully supported parents that are themselves fully or
/// partially supported
pub fn get_supported_data_variables(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
) -> Result<HashMap<ScopedVariable, DependencyNodeSupported>> {
    let (data_graph, _) = build_dependency_graph(chart_spec, config)?;
    // Sort dataset nodes topologically
    let nodes: Vec<NodeIndex> = toposort_dependency_graph(&data_graph)?;

    // Traverse nodes and save those to supported_vars that are supported with all supported
    // parents
    let mut all_supported_vars = HashMap::new();

    for node_index in &nodes {
        let (scoped_var, node_supported) = data_graph.node_weight(*node_index).unwrap();

        // Unsupported nodes not included
        if !matches!(node_supported, DependencyNodeSupported::Unsupported) {
            if matches!(node_supported, DependencyNodeSupported::Mirrored) {
                all_supported_vars.insert(scoped_var.clone(), node_supported.clone());
                continue;
            }

            // Signal and scale variables require all parents to be available.
            let all_parents_supported =
                all_parents_available(&data_graph, *node_index, &all_supported_vars);

            if all_parents_supported {
                all_supported_vars.insert(scoped_var.clone(), node_supported.clone());
            } else if matches!(scoped_var.0.namespace(), VariableNamespace::Data) {
                // Check if all input Datasets are supported. e.g. A data node with supported source
                // but some unsupported transform dependencies
                let all_dataset_inputs_supported =
                    all_data_parents_available(&data_graph, *node_index, &all_supported_vars);

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
    // of supported data/scale nodes. This is to avoid bringing over unnecessary signals.
    // Keep supported scales when scale copying is enabled so expressions can depend on them.
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
                if let Some(signal_supported) = all_supported_vars.get(scoped_var) {
                    if matches!(signal_supported, DependencyNodeSupported::Mirrored) {
                        supported_vars.insert(scoped_var.clone(), signal_supported.clone());
                        continue;
                    }

                    // Check if any dependent nodes are supported data sets
                    let mut dfs = petgraph::visit::Dfs::new(&data_graph, *node_index);
                    'dfs: while let Some(dfs_node_index) = dfs.next(&data_graph) {
                        let (dfs_scoped_var, _) = data_graph.node_weight(dfs_node_index).unwrap();
                        if all_supported_vars.contains_key(dfs_scoped_var)
                            && matches!(
                                dfs_scoped_var.0.namespace(),
                                VariableNamespace::Data | VariableNamespace::Scale
                            )
                        {
                            // Found supported child data/scale node. Add signal as supported and bail
                            // out of DFS
                            supported_vars.insert(scoped_var.clone(), signal_supported.clone());
                            break 'dfs;
                        }
                    }
                }
            }
            VariableNamespace::Scale => {
                if let Some(supported) = all_supported_vars.get(scoped_var) {
                    supported_vars.insert(scoped_var.clone(), supported.clone());
                }
            }
        }
    }

    Ok(supported_vars)
}

pub fn build_dependency_graph(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
) -> Result<(DependencyGraph, HashMap<ScopedVariable, NodeIndex>)> {
    let task_scope = chart_spec.to_task_scope()?;

    // Initialize graph with nodes
    let mut nodes_visitor = AddDependencyNodesVisitor::new(config, &task_scope, chart_spec);
    chart_spec.walk(&mut nodes_visitor)?;

    // Add dependency edges
    let mut edges_visitor = AddDependencyEdgesVisitor::new(
        &mut nodes_visitor.dependency_graph,
        &nodes_visitor.node_indexes,
        &task_scope,
    );
    chart_spec.walk(&mut edges_visitor)?;

    Ok((nodes_visitor.dependency_graph, nodes_visitor.node_indexes))
}

/// Visitor to initialize directed graph with nodes for each dataset (no edges yet)
#[derive(Debug)]
pub struct AddDependencyNodesVisitor<'a> {
    pub dependency_graph: DependencyGraph,
    pub node_indexes: HashMap<ScopedVariable, NodeIndex>,
    planner_config: &'a PlannerConfig,
    task_scope: &'a TaskScope,
    mark_index: u32,
}

impl<'a> AddDependencyNodesVisitor<'a> {
    pub fn new(
        planner_config: &'a PlannerConfig,
        task_scope: &'a TaskScope,
        chart_spec: &ChartSpec,
    ) -> Self {
        let mut dependency_graph = DiGraph::new();
        let mut node_indexes = HashMap::new();
        let width_available = root_dimension_is_numeric(chart_spec, "width");
        let height_available = root_dimension_is_numeric(chart_spec, "height");

        // Initialize with nodes for all built-in signals (e.g. width, height, etc.)
        for sig in BUILT_IN_SIGNALS.iter() {
            let supported =
                if (*sig == "width" && width_available) || (*sig == "height" && height_available) {
                    DependencyNodeSupported::Supported
                } else {
                    DependencyNodeSupported::Unsupported
                };
            let scoped_var = (Variable::new_signal(sig), Vec::new());
            let node_index = dependency_graph.add_node((scoped_var.clone(), supported));
            node_indexes.insert(scoped_var, node_index);
        }

        Self {
            dependency_graph,
            node_indexes,
            planner_config,
            task_scope,
            mark_index: 0,
        }
    }
}

impl ChartVisitor for AddDependencyNodesVisitor<'_> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Add scoped variable for dataset as node
        let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));
        let data_suported = if data.on.is_some() || data.is_selection_store() {
            DependencyNodeSupported::Mirrored
        } else {
            data.supported(self.planner_config, self.task_scope, scope)
        };
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
        let supported = if signal.bind.is_some() || !signal.on.is_empty() {
            DependencyNodeSupported::Mirrored
        } else {
            signal.supported(self.planner_config, self.task_scope, scope)
        };
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), supported));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let scoped_var = (Variable::new_scale(&scale.name), Vec::from(scope));
        let supported = if !self.planner_config.copy_scales_to_server {
            DependencyNodeSupported::Unsupported
        } else if !scale.signal_expressions_supported() {
            DependencyNodeSupported::Unsupported
        } else if !scale.server_domain_shape_supported() {
            DependencyNodeSupported::Unsupported
        } else if !scale.server_runtime_semantics_supported() {
            DependencyNodeSupported::Unsupported
        } else if scale.has_client_only_domain_sort() {
            DependencyNodeSupported::Unsupported
        } else if let Ok(input_vars) = scale.input_vars() {
            if input_vars.into_iter().any(|input_var| {
                if let Ok(scoped_source_var) =
                    scoped_var_for_input_var(&input_var, scope, self.task_scope)
                {
                    self.planner_config
                        .client_only_vars
                        .contains(&scoped_source_var)
                } else {
                    true
                }
            }) {
                DependencyNodeSupported::Unsupported
            } else {
                DependencyNodeSupported::Supported
            }
        } else {
            DependencyNodeSupported::Unsupported
        };
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), supported));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }

    fn visit_projection(&mut self, projection: &ProjectionSpec, scope: &[u32]) -> Result<()> {
        // Projections create scale variables
        let scoped_var = (Variable::new_scale(&projection.name), Vec::from(scope));
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }

    fn visit_non_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // non-group marks can serve as datasets
        let name = mark
            .name
            .clone()
            .unwrap_or_else(|| format!("unnamed_mark_{}", self.mark_index));
        self.mark_index += 1;

        let scoped_var = (Variable::new_data(&name), Vec::from(scope));
        let node_index = self
            .dependency_graph
            .add_node((scoped_var.clone(), DependencyNodeSupported::Unsupported));
        self.node_indexes.insert(scoped_var, node_index);

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
            let parent_scope = Vec::from(&scope[..(scope.len() - 1)]);
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
    pub dependency_graph: &'a mut DependencyGraph,
    node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
    task_scope: &'a TaskScope,
    mark_index: u32,
}

impl<'a> AddDependencyEdgesVisitor<'a> {
    pub fn new(
        dependency_graph: &'a mut DependencyGraph,
        node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
        task_scope: &'a TaskScope,
    ) -> Self {
        Self {
            dependency_graph,
            node_indexes,
            task_scope,
            mark_index: 0,
        }
    }
}

impl ChartVisitor for AddDependencyEdgesVisitor<'_> {
    /// Add edges into a data node
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Scoped var for this node
        let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing data node: {scoped_var:?}"))?;

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
                let Ok(scoped_source_var) =
                    scoped_var_for_input_var(&input_var, scope, self.task_scope)
                else {
                    continue;
                };
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
                .with_context(|| format!("Missing data node: {scoped_source_var:?}"))?;

            // Add directed edge
            self.dependency_graph
                .add_edge(*source_node_index, *node_index, ());
        }

        Ok(())
    }

    fn visit_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Facet datasets have parents
        if let Some(from) = &mark.from {
            if let Some(facet) = &from.facet {
                // Scoped var for this facet dataset
                let scoped_var = (Variable::new_data(&facet.name), Vec::from(scope));

                let node_index = self
                    .node_indexes
                    .get(&scoped_var)
                    .with_context(|| format!("Missing data node: {scoped_var:?}"))?;

                // Build scoped var for parent node
                let source = &facet.data;
                let source_var = Variable::new_data(source);
                // Resolve scope up one level because source dataset must be defined in parent,
                // not as a dataset within this group mark
                let resolved = self
                    .task_scope
                    .resolve_scope(&source_var, &scope[..(scope.len() - 1)])?;
                let scoped_source_var = (resolved.var, resolved.scope);

                let source_node_index = self
                    .node_indexes
                    .get(&scoped_source_var)
                    .with_context(|| format!("Missing data node: {scoped_source_var:?}"))?;

                // Add directed edge
                self.dependency_graph
                    .add_edge(*source_node_index, *node_index, ());
            }
        }

        Ok(())
    }

    fn visit_non_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // non-group marks can serve as datasets
        let name = mark
            .name
            .clone()
            .unwrap_or_else(|| format!("unnamed_mark_{}", self.mark_index));
        self.mark_index += 1;

        let Some(from) = &mark.from else {
            return Ok(());
        };
        let Some(source) = &from.data else {
            return Ok(());
        };

        // Scoped var for this facet dataset
        let scoped_var = (Variable::new_data(&name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing data node: {scoped_var:?}"))?;

        let source_var = Variable::new_data(source);
        // Resolve scope up one level because source dataset must be defined in parent,
        // not as a dataset within this group mark
        let resolved = self.task_scope.resolve_scope(&source_var, scope)?;
        let scoped_source_var = (resolved.var, resolved.scope);

        let source_node_index = self
            .node_indexes
            .get(&scoped_source_var)
            .with_context(|| format!("Missing data node: {scoped_source_var:?}"))?;

        // Add directed edge
        self.dependency_graph
            .add_edge(*source_node_index, *node_index, ());

        Ok(())
    }

    /// Add edges into a signal node
    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        // Scoped var for this node
        let scoped_var = (Variable::new_signal(&signal.name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing signal node: {scoped_var:?}"))?;

        let mut input_vars: Vec<InputVariable> = Vec::new();

        if let Some(update) = &signal.update {
            let expression = parse(update)?;
            input_vars.extend(expression.input_vars());
        }

        if let Some(init) = &signal.init {
            let expression = parse(init)?;
            input_vars.extend(expression.input_vars());
        }

        for input_var in input_vars {
            let Ok(scoped_source_var) =
                scoped_var_for_input_var(&input_var, scope, self.task_scope)
            else {
                continue;
            };
            let source_node_index = self
                .node_indexes
                .get(&scoped_source_var)
                .with_context(|| format!("Missing data node: {scoped_source_var:?}"))?;

            // Add directed edge
            self.dependency_graph
                .add_edge(*source_node_index, *node_index, ());
        }
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let scoped_var = (Variable::new_scale(&scale.name), Vec::from(scope));

        let node_index = self
            .node_indexes
            .get(&scoped_var)
            .with_context(|| format!("Missing scale node: {scoped_var:?}"))?;

        for input_var in scale.input_vars()? {
            let Ok(scoped_source_var) =
                scoped_var_for_input_var(&input_var, scope, self.task_scope)
            else {
                continue;
            };
            let source_node_index = self
                .node_indexes
                .get(&scoped_source_var)
                .with_context(|| format!("Missing dependency node: {scoped_source_var:?}"))?;

            self.dependency_graph
                .add_edge(*source_node_index, *node_index, ());
        }

        Ok(())
    }
}

fn root_dimension_is_numeric(chart_spec: &ChartSpec, name: &str) -> bool {
    chart_spec
        .extra
        .get(name)
        .and_then(|v| v.as_f64())
        .is_some()
        && !root_dimension_is_dynamic(chart_spec, name)
}

fn root_dimension_is_dynamic(chart_spec: &ChartSpec, name: &str) -> bool {
    let autosize = chart_spec.extra.get("autosize");
    let autosize_type = match autosize {
        Some(serde_json::Value::String(kind)) => kind.as_str(),
        Some(serde_json::Value::Object(obj)) => obj
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("pad"),
        _ => "pad",
    };

    match autosize_type {
        "fit" => true,
        "fit-x" => name == "width",
        "fit-y" => name == "height",
        _ => false,
    }
}

fn all_parents_available(
    data_graph: &DependencyGraph,
    node_index: NodeIndex,
    all_supported_vars: &HashMap<ScopedVariable, DependencyNodeSupported>,
) -> bool {
    data_graph
        .edges_directed(node_index, Incoming)
        .map(|edge| data_graph.node_weight(edge.source()).unwrap().0.clone())
        .all(|parent_var| {
            matches!(
                all_supported_vars.get(&parent_var),
                Some(DependencyNodeSupported::Supported) | Some(DependencyNodeSupported::Mirrored)
            )
        })
}

fn all_data_parents_available(
    data_graph: &DependencyGraph,
    node_index: NodeIndex,
    all_supported_vars: &HashMap<ScopedVariable, DependencyNodeSupported>,
) -> bool {
    data_graph
        .edges_directed(node_index, Incoming)
        .map(|edge| data_graph.node_weight(edge.source()).unwrap().0.clone())
        .all(|parent_var| {
            if !matches!(parent_var.0.namespace(), VariableNamespace::Data) {
                return true;
            }

            matches!(
                all_supported_vars.get(&parent_var),
                Some(DependencyNodeSupported::Supported) | Some(DependencyNodeSupported::Mirrored)
            )
        })
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

#[cfg(test)]
mod tests {
    use super::{build_dependency_graph, get_supported_data_variables};
    use crate::planning::plan::PlannerConfig;
    use crate::proto::gen::tasks::Variable;
    use crate::spec::chart::ChartSpec;
    use crate::spec::data::DependencyNodeSupported;
    use serde_json::json;

    fn chart_with_scale_and_signal() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "scales": [
                {"name": "x"}
            ],
            "signals": [
                {"name": "s", "update": "scale('x', 1)"}
            ]
        }))
        .unwrap()
    }

    fn chart_with_scale_domain_raw_and_named_range() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": 300,
            "signals": [
                {"name": "raw_domain", "value": [0, 1]}
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "linear",
                    "domain": [0, 1],
                    "domainRaw": {"signal": "raw_domain"},
                    "range": "width"
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_client_only_scale_sort() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "domain_source",
                    "values": [
                        {"k": "b", "other": 2},
                        {"k": "a", "other": 1}
                    ]
                },
                {
                    "name": "source",
                    "values": [
                        {"v": 1},
                        {"v": 2}
                    ],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.v)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "band",
                    "domain": {
                        "data": "domain_source",
                        "field": "k",
                        "sort": {"field": "other", "order": "descending"}
                    },
                    "range": [0, 100]
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_unsupported_scale_signal_expression() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": 200,
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 1}],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.v)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "linear",
                    "domain": [0, 1],
                    "range": [0, {"signal": "foo(width)"}]
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_unsupported_scale_domain_shape() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"a": 1}]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "linear",
                    "domain": {
                        "fields": [
                            {"data": "source", "field": "a"},
                            [{"expr": "1"}]
                        ]
                    },
                    "range": [0, 100]
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_unsupported_scale_runtime_semantics() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 1}],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.v)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "linear",
                    "domain": [0, 1],
                    "domainMid": 0.5,
                    "range": [0, 100]
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_unsupported_time_scale_range_semantics() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [
                        {"t": "2020-01-01T00:00:00.000Z"}
                    ]
                }
            ],
            "signals": [
                {"name": "s", "update": "scale('color', now())"}
            ],
            "scales": [
                {
                    "name": "color",
                    "type": "time",
                    "domain": {"data": "source", "field": "t"},
                    "range": "ramp"
                }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn test_scale_nodes_supported_only_when_copy_scales_enabled() {
        let chart_spec = chart_with_scale_and_signal();
        let scale_var = (Variable::new_scale("x"), Vec::new());

        let mut no_scales = PlannerConfig::default();
        no_scales.copy_scales_to_server = false;
        let (graph_no_scales, index_no_scales) =
            build_dependency_graph(&chart_spec, &no_scales).unwrap();
        let (_, supported_no_scales) = graph_no_scales
            .node_weight(*index_no_scales.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(
            supported_no_scales,
            DependencyNodeSupported::Unsupported
        ));

        let mut with_scales = PlannerConfig::default();
        with_scales.copy_scales_to_server = true;
        let (graph_with_scales, index_with_scales) =
            build_dependency_graph(&chart_spec, &with_scales).unwrap();
        let (_, supported_with_scales) = graph_with_scales
            .node_weight(*index_with_scales.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(
            supported_with_scales,
            DependencyNodeSupported::Supported
        ));
    }

    #[test]
    fn test_signal_update_dependencies_include_scales() {
        let chart_spec = chart_with_scale_and_signal();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let scale_node = *node_indexes
            .get(&(Variable::new_scale("x"), Vec::new()))
            .unwrap();
        let signal_node = *node_indexes
            .get(&(Variable::new_signal("s"), Vec::new()))
            .unwrap();

        assert!(graph.contains_edge(scale_node, signal_node));
    }

    #[test]
    fn test_supported_vars_include_scales_when_copy_enabled() {
        let chart_spec = chart_with_scale_and_signal();
        let scale_var = (Variable::new_scale("x"), Vec::new());

        let mut no_scales = PlannerConfig::default();
        no_scales.copy_scales_to_server = false;
        let supported_no_scales = get_supported_data_variables(&chart_spec, &no_scales).unwrap();
        assert!(!supported_no_scales.contains_key(&scale_var));

        let mut with_scales = PlannerConfig::default();
        with_scales.copy_scales_to_server = true;
        let supported_with_scales =
            get_supported_data_variables(&chart_spec, &with_scales).unwrap();
        assert!(matches!(
            supported_with_scales.get(&scale_var),
            Some(DependencyNodeSupported::Supported)
        ));
    }

    #[test]
    fn test_scale_edges_include_domain_raw_and_named_range_dependencies() {
        let chart_spec = chart_with_scale_domain_raw_and_named_range();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();

        let scale_node = *node_indexes
            .get(&(Variable::new_scale("x"), Vec::new()))
            .unwrap();
        let domain_raw_node = *node_indexes
            .get(&(Variable::new_signal("raw_domain"), Vec::new()))
            .unwrap();
        let width_node = *node_indexes
            .get(&(Variable::new_signal("width"), Vec::new()))
            .unwrap();

        assert!(graph.contains_edge(domain_raw_node, scale_node));
        assert!(graph.contains_edge(width_node, scale_node));
    }

    #[test]
    fn test_scale_with_unavailable_root_width_not_supported() {
        let chart_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 1], "range": "width"}
            ]
        }))
        .unwrap();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&(Variable::new_scale("x"), Vec::new())));
    }

    #[test]
    fn test_scale_with_autosize_fit_x_root_width_not_supported() {
        let chart_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": 200,
            "autosize": {"type": "fit-x", "contains": "padding"},
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 1], "range": "width"}
            ]
        }))
        .unwrap();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&(Variable::new_scale("x"), Vec::new())));
    }

    #[test]
    fn test_scale_with_non_aggregated_sort_field_without_op_is_client_only() {
        let chart_spec = chart_with_client_only_scale_sort();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let scale_var = (Variable::new_scale("x"), Vec::new());
        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let (_, supported) = graph
            .node_weight(*node_indexes.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(supported, DependencyNodeSupported::Unsupported));

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&scale_var));
    }

    #[test]
    fn test_scale_with_unsupported_signal_expression_is_client_only() {
        let chart_spec = chart_with_unsupported_scale_signal_expression();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let scale_var = (Variable::new_scale("x"), Vec::new());
        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let (_, supported) = graph
            .node_weight(*node_indexes.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(supported, DependencyNodeSupported::Unsupported));

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&scale_var));
    }

    #[test]
    fn test_scale_with_unsupported_domain_shape_is_client_only() {
        let chart_spec = chart_with_unsupported_scale_domain_shape();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let scale_var = (Variable::new_scale("x"), Vec::new());
        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let (_, supported) = graph
            .node_weight(*node_indexes.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(supported, DependencyNodeSupported::Unsupported));

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&scale_var));
    }

    #[test]
    fn test_scale_with_unsupported_runtime_semantics_is_client_only() {
        let chart_spec = chart_with_unsupported_scale_runtime_semantics();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let scale_var = (Variable::new_scale("x"), Vec::new());
        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let (_, supported) = graph
            .node_weight(*node_indexes.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(supported, DependencyNodeSupported::Unsupported));

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&scale_var));
    }

    #[test]
    fn test_time_scale_with_non_numeric_range_is_client_only() {
        let chart_spec = chart_with_unsupported_time_scale_range_semantics();
        let mut config = PlannerConfig::default();
        config.copy_scales_to_server = true;

        let scale_var = (Variable::new_scale("color"), Vec::new());
        let (graph, node_indexes) = build_dependency_graph(&chart_spec, &config).unwrap();
        let (_, supported) = graph
            .node_weight(*node_indexes.get(&scale_var).unwrap())
            .unwrap();
        assert!(matches!(supported, DependencyNodeSupported::Unsupported));

        let supported_vars = get_supported_data_variables(&chart_spec, &config).unwrap();
        assert!(!supported_vars.contains_key(&scale_var));
    }
}
