/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use std::collections::{HashMap, HashSet};

pub type VlSelectionFields = HashMap<ScopedVariable, ColumnUsage>;

/// Enum storing info on which dataset columns are used in a given context.
/// Due to the dynamic nature of Vega specifications, it's not always possible to statically
/// determine which columns from a dataset will be used at runtime. In this case the
/// ColumnUsage::Unknown variant is used.  In the context of projection pushdown,
/// the ColumnUsage::Unknown variant indicates that all of original dataset columns must be
/// maintained
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnUsage {
    Unknown,
    Known(HashSet<String>),
}

impl ColumnUsage {
    pub fn empty() -> ColumnUsage {
        ColumnUsage::Known(Default::default())
    }

    pub fn with_column(&self, column: &str) -> ColumnUsage {
        self.union(&ColumnUsage::from(vec![column].as_slice()))
    }

    /// Take the union of two ColumnUsage instances. If both are ColumnUsage::Known, then take
    /// the union of their known columns. If either is ColumnUsage::Unknown, then the union is
    /// also Unknown.
    pub fn union(&self, other: &ColumnUsage) -> ColumnUsage {
        match (self, other) {
            (ColumnUsage::Known(self_cols), ColumnUsage::Known(other_cols)) => {
                // If both column usages are known, we can union the known columns
                let new_cols: HashSet<_> = self_cols.union(other_cols).cloned().collect();
                ColumnUsage::Known(new_cols)
            }
            _ => {
                // If either is Unknown, then the union is unknown
                ColumnUsage::Unknown
            }
        }
    }

    pub fn difference(&self, other: &ColumnUsage) -> ColumnUsage {
        match (self, other) {
            (ColumnUsage::Known(self_cols), ColumnUsage::Known(other_cols)) => {
                // If both column usages are known, we can take the set difference the known columns
                let new_cols: HashSet<_> = self_cols.difference(other_cols).cloned().collect();
                ColumnUsage::Known(new_cols)
            }
            _ => {
                // If either is Unknown, then the difference is unknown
                ColumnUsage::Unknown
            }
        }
    }
}

impl From<&str> for ColumnUsage {
    fn from(column: &str) -> Self {
        let columns: HashSet<_> = vec![column.to_string()].into_iter().collect();
        Self::Known(columns)
    }
}

impl From<&[&str]> for ColumnUsage {
    fn from(columns: &[&str]) -> Self {
        let columns: HashSet<_> = columns.iter().map(|s| s.to_string()).collect();
        Self::Known(columns)
    }
}

impl From<&[String]> for ColumnUsage {
    fn from(columns: &[String]) -> Self {
        let columns: HashSet<_> = columns.iter().cloned().collect();
        Self::Known(columns)
    }
}

/// Struct that tracks the usage of all columns across a collection of datasets
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetsColumnUsage {
    pub usages: HashMap<ScopedVariable, ColumnUsage>,
    pub aliases: HashMap<ScopedVariable, ScopedVariable>,
}

impl DatasetsColumnUsage {
    pub fn empty() -> Self {
        Self {
            usages: Default::default(),
            aliases: Default::default(),
        }
    }

    pub fn with_column_usage(&self, datum_var: &ScopedVariable, usage: ColumnUsage) -> Self {
        let other_column_usage = Self {
            usages: vec![(datum_var.clone(), usage)].into_iter().collect(),
            aliases: Default::default(),
        };
        self.union(&other_column_usage)
    }

    pub fn with_unknown_usage(&self, datum_var: &ScopedVariable) -> Self {
        self.with_column_usage(datum_var, ColumnUsage::Unknown)
    }

    pub fn without_column_usage(&self, datum_var: &ScopedVariable, usage: &ColumnUsage) -> Self {
        let mut new_usages = self.usages.clone();
        if let Some(current_usage) = new_usages.get(datum_var) {
            let new_usage = current_usage.difference(usage);
            new_usages.insert(datum_var.clone(), new_usage);
        }
        Self {
            usages: new_usages,
            aliases: self.aliases.clone(),
        }
    }

    pub fn with_alias(&self, from: ScopedVariable, to: ScopedVariable) -> Self {
        let mut aliases = self.aliases.clone();
        aliases.insert(from, to);
        Self {
            usages: self.usages.clone(),
            aliases,
        }
    }

    /// Take the union of two DatasetColumnUsage instances.
    pub fn union(&self, other: &DatasetsColumnUsage) -> DatasetsColumnUsage {
        let self_vars: HashSet<_> = self.usages.keys().cloned().collect();
        let other_vars: HashSet<_> = other.usages.keys().cloned().collect();
        let union_vars: HashSet<_> = self_vars.union(&other_vars).cloned().collect();

        // Union aliases
        let mut aliases = self.aliases.clone();
        for (key, val) in &other.aliases {
            aliases.insert(key.clone(), val.clone());
        }

        let mut usages: HashMap<ScopedVariable, ColumnUsage> = HashMap::new();
        for var in union_vars {
            // Check if var is an alias
            let var = aliases.get(&var).unwrap_or(&var).clone();

            let self_usage = self
                .usages
                .get(&var)
                .cloned()
                .unwrap_or_else(ColumnUsage::empty);
            let other_usage = other
                .usages
                .get(&var)
                .cloned()
                .unwrap_or_else(ColumnUsage::empty);
            let combined_usage = self_usage.union(&other_usage);
            usages.insert(var, combined_usage);
        }

        Self { usages, aliases }
    }
}

pub trait GetDatasetsColumnUsage {
    fn datasets_column_usage(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage;
}

#[cfg(test)]
mod tests {
    use crate::expression::column_usage::ColumnUsage;

    #[test]
    fn test_with_column() {
        let left = ColumnUsage::from(vec!["one", "two"].as_slice());
        let result = left.with_column("three").with_column("four");
        let expected = ColumnUsage::from(vec!["one", "two", "three", "four"].as_slice());
        assert_eq!(result, expected)
    }

    #[test]
    fn test_union_known_known() {
        let left = ColumnUsage::from(vec!["one", "two"].as_slice());
        let right = ColumnUsage::from(vec!["two", "three", "four"].as_slice());
        let union = left.union(&right);
        let expected = ColumnUsage::from(vec!["one", "two", "three", "four"].as_slice());
        assert_eq!(union, expected)
    }

    #[test]
    fn test_union_known_unknown() {
        let left = ColumnUsage::from(vec!["one", "two"].as_slice());
        let union = left.union(&ColumnUsage::Unknown);
        assert_eq!(union, ColumnUsage::Unknown)
    }

    #[test]
    fn test_union_unknown_known() {
        let right = ColumnUsage::from(vec!["two", "three", "four"].as_slice());
        let union = ColumnUsage::Unknown.union(&right);
        assert_eq!(union, ColumnUsage::Unknown)
    }

    #[test]
    fn test_union_unknown_unknown() {
        let union = ColumnUsage::Unknown.union(&ColumnUsage::Unknown);
        assert_eq!(union, ColumnUsage::Unknown)
    }

    #[test]
    fn test_difference_known_known() {
        let left = ColumnUsage::from(vec!["one", "two", "three"].as_slice());
        let right = ColumnUsage::from(vec!["three", "four"].as_slice());
        let union = left.difference(&right);
        let expected = ColumnUsage::from(vec!["one", "two"].as_slice());
        assert_eq!(union, expected)
    }

    #[test]
    fn test_difference_known_unknown() {
        let left = ColumnUsage::from(vec!["one", "two"].as_slice());
        let union = left.difference(&ColumnUsage::Unknown);
        assert_eq!(union, ColumnUsage::Unknown)
    }

    #[test]
    fn test_difference_unknown_known() {
        let right = ColumnUsage::from(vec!["two", "three", "four"].as_slice());
        let union = ColumnUsage::Unknown.difference(&right);
        assert_eq!(union, ColumnUsage::Unknown)
    }

    #[test]
    fn test_difference_unknown_unknown() {
        let union = ColumnUsage::Unknown.difference(&ColumnUsage::Unknown);
        assert_eq!(union, ColumnUsage::Unknown)
    }
}
