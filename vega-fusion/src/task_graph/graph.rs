use std::collections::{HashMap, HashSet};
use crate::variable::ScopedVariable;
use crate::task_graph::task::{Task, TaskValue};
use crate::error::{Result, VegaFusionError};

#[derive(Clone)]
pub struct TaskEdge {
    pub source: ScopedVariable,
    pub destination: ScopedVariable,
    pub output_index: Option<usize>,
}

/// Task graph is an immutable representation of a Vega specification.  It does not change
/// as the values of nodes are updated
#[derive(Clone)]
pub struct TaskGraph {
    tasks: HashMap<ScopedVariable, Task>,
    incoming: HashMap<ScopedVariable, Vec<TaskEdge>>,
    outgoing: HashMap<ScopedVariable, Vec<TaskEdge>>,
    ident_hash: HashMap<ScopedVariable, u64>,
}


/// Representation of the current state of a task graph.
/// The current value of each root node in the task graph is stored in root_state.
///
/// A deterministic hash is computed for all of the nodes in the graph based on their identity
/// and the state and identify of their parents.
///
/// The TaskGraphState is designed to be used with a transient cache where the keys
/// are based on the ident_hash and state_hash of the nodes. The combination of a TaskGraph and
/// TaskGraphState is sufficient to calculate the value of any node in the task graph, so it
/// is fine for arbitrary entries to be evicted.
#[derive(Clone, Debug)]
pub struct TaskGraphState {
    /// Hash for the current state of every node
    state_hash: HashMap<ScopedVariable, u64>,

    /// Current value of each root (parentless) task in the graph.
    /// These values are intended to be small.
    root_values: HashMap<ScopedVariable, TaskValue>,
}

impl TaskGraph {
    // new from tasks and edges?
    pub fn new(tasks: HashMap<ScopedVariable, Task>, edges: Vec<TaskEdge>) -> Result<Self> {
        let mut incoming: HashMap<ScopedVariable, Vec<TaskEdge>> = Default::default();
        let mut outgoing: HashMap<ScopedVariable, Vec<TaskEdge>> = Default::default();

         for edge in edges {
             if !tasks.contains_key(&edge.destination) {
                 return Err(VegaFusionError::specification(
                     &format!("Invalid scope variable reference: {:?}", edge.destination)
                 ))
             }
             if !tasks.contains_key(&edge.source) {
                 return Err(VegaFusionError::specification(
                     &format!("Invalid scope variable reference: {:?}", edge.destination)
                 ))
             }

             incoming.entry(edge.destination).or_default().push(edge.clone());
             outgoing.entry(edge.source).or_default().push(edge.clone());
         }

        // TODO: initialize ident_hash

        Ok(Self {
            tasks,
            incoming,
            outgoing,
            ident_hash: Default::default()
        })
    }

    pub fn root_variables(&self) -> Vec<ScopedVariable> {
        self.tasks.keys().filter(|v| self.incoming.is_empty()).collect()
    }

    pub fn send(
        &self,
        state: &mut TaskGraphState,
        values: Vec<(ScopedVariable, TaskValue)>,
        watch: &Vec<ScopedVariable>,
        cache: &mut dyn TaskGraphCache,
    ) -> Result<(WatchValues, CacheValues)> {

        // HashSet of variables that have been updated during this traversal
        let mut updated_vars: HashSet<ScopedVariable> = HashSet::new();

        // Update sent values if on the
        for (variable, value) in values {
            state.root_values.insert(variable.clone(), value);

            // Save off id of update task value
            updated_vars.insert(variable.clone());
        }

        // TODO: update state hashes


        todo!()
    }
}

type WatchValues = HashMap<ScopedVariable, TaskValue>;
type CacheValues = HashMap<(u64, u64), TaskValue>;

pub trait TaskGraphCache {
    fn get(&mut self, id_hash: u64, state_hash: u64) -> Option<TaskValue>;
}