/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

impl Variable {
    pub fn new(ns: VariableNamespace, name: &str) -> Self {
        if name.contains(':') {
            panic!("Variable names may not contain colons")
        }
        Self {
            namespace: ns as i32,
            name: String::from(name),
        }
    }

    pub fn new_signal(id: &str) -> Self {
        Self::new(VariableNamespace::Signal, id)
    }

    pub fn new_scale(id: &str) -> Self {
        Self::new(VariableNamespace::Scale, id)
    }

    pub fn new_data(id: &str) -> Self {
        Self::new(VariableNamespace::Data, id)
    }

    pub fn ns(&self) -> VariableNamespace {
        VariableNamespace::from_i32(self.namespace).unwrap()
    }
}

impl Eq for Variable {}

impl PartialOrd for Variable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Variable {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.namespace == other.namespace {
            self.name.cmp(&other.name)
        } else {
            self.namespace.cmp(&other.namespace)
        }
    }
}

// The Prost structs derive PartialEq but not Hash, so we need to implement Hash here.
// This is a bad idea in general since PartialEq and Hash must be consistent, but there's
// not a prost option to disable deriving PartialEq, or to derive Hash. This Hash implementation
// is simple enough that the risk of inconsistency is low.
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Variable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.name.hash(state);
    }
}

// A variable with scope.  In a Vega spec, variables may be defined at the top-level, or they
// may be defined inside nested group marks. The scope is a `Vec<usize>` and it encodes the level
// at which the variable is defined.
//   - An empty scope corresponds to a variable defined at the top level of the specification.
//   - Each element of the scope vector represents a level of nesting, where the integer is the
//     index into the collection of group marks.
//
// A scope of `vec![0, 1]` means that the variable is defined in a group mark that is nested two
// levels deep. At the top level, the first group mark is chosen. At the second level,
// the second group mark is chosen.
// #[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Hash, Ord)]
// pub struct ScopedVariable {
//     pub variable: Variable,
//     pub scope: Vec<usize>,
// }
//
// impl ScopedVariable {
//     pub fn new(ns: VariableNamespace, name: &str, scope: Vec<usize>) -> Self {
//         Self {
//             variable: Variable::new(ns, name),
//             scope,
//         }
//     }
//
//     pub fn new_signal(id: &str, scope: Vec<usize>) -> Self {
//         Self::new(VariableNamespace::Signal, id, scope)
//     }
//
//     pub fn new_scale(id: &str, scope: Vec<usize>) -> Self {
//         Self::new(VariableNamespace::Scale, id, scope)
//     }
//
//     pub fn new_data(id: &str, scope: Vec<usize>) -> Self {
//         Self::new(VariableNamespace::Data, id, scope)
//     }
// }
