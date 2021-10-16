use crate::proto::gen::tasks::{TaskGraph, Task, Variable, TaskNode, OutgoingEdge, IncomingEdge, ScanUrlTask, TransformsTask};
use crate::task_graph::scope::TaskScope;
use crate::error::{Result, ResultWithContext, ToInternalError, VegaFusionError};
use std::collections::HashMap;
use petgraph::graph::NodeIndex;
use petgraph::algo::toposort;
use petgraph::Direction;
use petgraph::prelude::EdgeRef;
use itertools::Itertools;
use crate::proto::gen::transforms::{TransformPipeline, Transform, Extent};
use crate::proto::gen::transforms::transform::TransformKind;
use crate::task_graph::task_value::TaskValue;
use crate::data::scalar::ScalarValue;


// pub struct TaskGraphBuilder {
//     task_scope: TaskScope,
//     tasks: Vec<Task>,
// }
//
// impl TaskGraphBuilder {
//     pub fn new(task_scope: TaskScope) -> Self {
//         Self { task_scope, tasks: Default::default() }
//     }
//
//     pub fn add_task(&mut self, task: Task) {
//         self.tasks.push(task);
//     }
//
//     pub fn build(&self) -> TaskGraph {
//         todo!()
//     }
// }

struct PetgraphEdge { signal: Option<String> }

type ScopedVariable = (Variable, Vec<u32>);

impl TaskGraph {
    pub fn new(tasks: Vec<Task>, task_scope: TaskScope) -> Result<Self> {

        let mut graph: petgraph::graph::DiGraph<ScopedVariable, PetgraphEdge> = petgraph::graph::DiGraph::new();
        let mut tasks_map: HashMap<ScopedVariable, (NodeIndex, Task)> = HashMap::new();

        // Add graph nodes
        for task in tasks {
            // Add scope variable
            let scoped_var = (task.variable().clone(), task.scope.clone());
            let node_index = graph.add_node(scoped_var.clone());
            tasks_map.insert(scoped_var, (node_index, task));
        }

        // Resolve and add edges
        for (node_index, task) in tasks_map.values() {
            let usage_scope = task.scope();
            for input_var in task.input_vars() {
                let resolved = task_scope.resolve_scope(&input_var, usage_scope)?;
                let input_scoped_var = (resolved.var.clone(), resolved.scope.clone());
                let (input_node_index, _) = tasks_map.get(&input_scoped_var).with_context(
                    || format!("No variable {:?} with scope {:?}", input_scoped_var.0, input_scoped_var.1)
                )?;

                // Add graph edge
                graph.add_edge(
                    input_node_index.clone(),
                    node_index.clone(),
                    PetgraphEdge {
                        signal: resolved.signal.clone()
                    }
                );
            }
        }

        // Create mapping from toposorted node_index to the final linear node index
        let toposorted: Vec<NodeIndex> = match toposort(&graph, None) {
            Err(err) => return Err(VegaFusionError::internal("failed to sort dependency graph topologically")),
            Ok(toposorted) => toposorted
        };

        let toposorted_node_indexes: HashMap<NodeIndex, usize> = toposorted.iter().enumerate().map(
            |(sorted_index, node_index)| (*node_index, sorted_index)
        ).collect();

        // Create linear vec of TaskNodes, with edges as sorted index references to nodes
        let task_nodes = toposorted.iter().map(|node_index| {
            let scoped_var = graph.node_weight(*node_index).unwrap();
            let (_, task) = tasks_map.get(scoped_var).unwrap();
        
            // Collect outgoing node indexes
            let outgoing_node_ids: Vec<_> = graph.edges_directed(*node_index, Direction::Outgoing).map(
                |edge| edge.target()
            ).collect();

            let outgoing: Vec<_> = outgoing_node_ids.iter().map(
                |node_index| {
                    let sorted_index = *toposorted_node_indexes.get(node_index).unwrap() as u32;
                    OutgoingEdge {
                        target: sorted_index
                    }
                }
            ).collect();
            
            // Collect incoming node indexes
            let incoming_node_ids: Vec<_> = graph.edges_directed(*node_index, Direction::Incoming).map(
                |edge| (edge.source(), &edge.weight().signal)
            ).collect();

            // Sort incoming nodes to match order expected by the task
            let incoming_vars: HashMap<_, _> = incoming_node_ids.iter().map(|(node_index, signal)| {
                let var = graph.node_weight(*node_index).unwrap().0.clone();
                (var, (node_index, signal.clone()))
            }).collect();

            let incoming: Vec<_> = task.input_vars().iter().map(|var| {
                let (node_index, signal) = *incoming_vars.get(var).unwrap();
                let sorted_index = *toposorted_node_indexes.get(node_index).unwrap() as u32;

                if let Some(signal) = signal {
                    let weight = graph.node_weight(*node_index).unwrap();
                    let (_, input_task) = tasks_map.get(weight).unwrap();
                    let signal_index = input_task.output_signals().iter().position(|e| e == signal).with_context(
                        || "Failed to find signal"
                    )?;
                    Ok(IncomingEdge {
                        source: sorted_index,
                        signal: Some(signal_index as u32)
                    })
                } else {
                    Ok(IncomingEdge {
                        source: sorted_index,
                        signal: None
                    })
                }
            }).collect::<Result<Vec<_>>>()?;

            Ok(TaskNode {
                task: Some(task.clone()),
                incoming,
                outgoing,
                id_fingerprint: 0,
                state_fingerprint: 0
            })
        }).collect::<Result<Vec<_>>>()?;

        Ok(Self {
            nodes: task_nodes
        })
    }
}


#[test]
fn try_it() {
    let mut task_scope = TaskScope::new();
    task_scope.add_variable(&Variable::new_signal("url"), Default::default());
    task_scope.add_variable(&Variable::new_data("url_datasetA"), Default::default());
    task_scope.add_variable(&Variable::new_data("datasetA"), Default::default());
    task_scope.add_data_signal("datasetA", "my_extent", Default::default());

    let tasks = vec![
        Task::new_value(
            Variable::new_signal("url"),
            Default::default(),
            TaskValue::Scalar(ScalarValue::from("file:///somewhere/over/the/rainbow.csv")),
        ),
        Task::new_scan_url(Variable::new_data("url_datasetA"), Default::default(), ScanUrlTask {
            url: Some(Variable::new_signal("url")),
            batch_size: 1024,
            format_type: None
        }),
        Task::new_transforms(Variable::new_signal("datasetA"), Default::default(), TransformsTask {
            source: "url_datasetA".to_string(),
            pipeline: Some(TransformPipeline { transforms: vec![
                Transform { transform_kind: Some(TransformKind::Extent(Extent {
                    field: "col_1".to_string(),
                    signal: Some("my_extent".to_string()),
                })) }
            ] })
        })
    ];

    let graph = TaskGraph::new(tasks, task_scope).unwrap();

    println!("graph:\n{:#?}", graph);
}