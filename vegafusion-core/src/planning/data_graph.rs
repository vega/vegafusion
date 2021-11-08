use std::collections::HashMap;
use petgraph::algo::toposort;
use petgraph::Incoming;
use petgraph::prelude::{DiGraph, EdgeRef, NodeIndex};
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataSpec, DataSupported};
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task_graph::ScopedVariable;


/// get HashSet of all data variables with fully supported parents that are themselves fully or
/// partially supported
pub fn get_supported_data_variables(chart_spec: &ChartSpec) -> Result<HashMap<ScopedVariable, DataSupported>> {
    let data_graph = build_data_graph(chart_spec)?;
    // Sort dataset nodes topologically
    let nodes: Vec<NodeIndex> = match toposort(&data_graph, None) {
        Ok(v) => v,
        Err(err) => {
            return Err(VegaFusionError::internal(format!("Failed to sort datasets topologically: {:?}", err)))
        }
    };

    // Traverse nodes and save those to supported_vars that are supported with all supported
    // parents
    let mut supported_vars = HashMap::new();

    for node_index in nodes {
        let (scoped_var, node_supported) = data_graph.node_weight(node_index).unwrap();

        // Unsupported nodes not included
        if !matches!(node_supported, DataSupported::Unsupported) {
            let parent_vars: Vec<_> = data_graph
                .edges_directed(node_index, Incoming)
                .into_iter()
                .map(|edge| {
                    data_graph.node_weight(edge.source()).unwrap().0.clone()
                })
                .collect();

            // Check whether all parents are fully supported
            let all_parents_supported = parent_vars.into_iter().all(|parent_var| {
                match supported_vars.get(&parent_var) {
                    Some(DataSupported::Supported) => true,
                    _ => false,
                }
            });

            if all_parents_supported {
                supported_vars.insert(scoped_var.clone(), node_supported.clone());
            }
        }
    }

    Ok(supported_vars)
}

pub fn build_data_graph(chart_spec: &ChartSpec) -> Result<DiGraph<(ScopedVariable, DataSupported), ()>> {
    // Initialize graph with nodes
    let mut nodes_visitor = AddDataNodesVisitor::new();
    chart_spec.walk(&mut nodes_visitor)?;

    // Add dependency edges
    let task_scope = chart_spec.to_task_scope()?;
    let mut edges_visitor = AddDataEdgesVisitor::new(
        &mut nodes_visitor.data_graph, &nodes_visitor.node_indexes, &task_scope
    );
    chart_spec.walk(&mut edges_visitor)?;

    Ok(nodes_visitor.data_graph)
}


/// Visitor to initialize directed graph with nodes for each dataset (no edges yet)
#[derive(Debug, Default)]
pub struct AddDataNodesVisitor {
    pub data_graph: DiGraph<(ScopedVariable, DataSupported), ()>,
    node_indexes: HashMap<ScopedVariable, NodeIndex>,
}

impl AddDataNodesVisitor {
    pub fn new() -> Self {
        Self {
            data_graph: DiGraph::new(),
            node_indexes: HashMap::new(),
        }
    }
}

impl ChartVisitor for AddDataNodesVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Add scoped variable for dataset as node
        let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));
        let node_index = self.data_graph.add_node((scoped_var.clone(), data.supported()));
        self.node_indexes.insert(scoped_var, node_index);
        Ok(())
    }
}


/// Visitor to add directed edges to graph with data nodes
#[derive(Debug)]
pub struct AddDataEdgesVisitor<'a> {
    pub data_graph: &'a mut DiGraph<(ScopedVariable, DataSupported), ()>,
    node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
    task_scope: &'a TaskScope,
}

impl <'a> AddDataEdgesVisitor<'a> {
    pub fn new(
        data_graph: &'a mut DiGraph<(ScopedVariable, DataSupported), ()>,
        node_indexes: &'a HashMap<ScopedVariable, NodeIndex>,
        task_scope: &'a TaskScope,
    ) -> Self {
        Self {
            data_graph, task_scope, node_indexes
        }
    }
}

impl <'a> ChartVisitor for AddDataEdgesVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {

        if let Some(source) = &data.source {
            // Scoped var for this node
            let scoped_var = (Variable::new_data(&data.name), Vec::from(scope));

            // Build scoped var for parent node
            let source_var = Variable::new_data(source);
            let resolved = self.task_scope.resolve_scope(&source_var, scope)?;
            let scoped_source_var = (resolved.var, resolved.scope);

            // Get node indexes
            let node_index = self.node_indexes.get(&scoped_var).with_context(
                || format!("Missing data node: {:?}", scoped_var)
            )?;
            let source_node_index = self.node_indexes.get(&scoped_source_var).with_context(
                || format!("Missing data node: {:?}", scoped_source_var)
            )?;

            // Add directed edge
            self.data_graph.add_edge(*source_node_index, *node_index, ());
        }

        Ok(())
    }
}
