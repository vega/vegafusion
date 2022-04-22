/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;

use std::collections::HashSet;

/// This optimization pass examines data nodes that have been planned to execute on the server.
/// For URL data nodes, if the node has external input vars in it's transforms, it is split
/// into an upper part that has no dependencies. This way, the data isn't loaded from the external
/// url repeatedly when the values of the input variables changes
pub fn split_data_url_nodes(spec: &mut ChartSpec) -> Result<()> {
    let mut visitor = SplitUrlDataNodeVisitor::new();
    spec.walk_mut(&mut visitor)?;

    for (parent_data, scope) in visitor.parent_url_data_nodes {
        // Add parent data at appropriate scope
        if scope.is_empty() {
            // Add parent data node to spec
            spec.data.push(parent_data)
        } else {
            // Add parent data node to spec
            let parent_group = spec.get_nested_group_mut(scope.as_slice())?;
            parent_group.data.push(parent_data);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct SplitUrlDataNodeVisitor {
    pub parent_url_data_nodes: Vec<(DataSpec, Vec<u32>)>,
}

impl SplitUrlDataNodeVisitor {
    pub fn new() -> Self {
        Self {
            parent_url_data_nodes: Default::default(),
        }
    }
}

impl MutChartVisitor for SplitUrlDataNodeVisitor {
    fn visit_data(&mut self, data: &mut DataSpec, scope: &[u32]) -> crate::error::Result<()> {
        if data.url.is_some() {
            let mut pipeline_vars: HashSet<Variable> = HashSet::new();
            let mut num_supported = 0;
            for (i, tx) in data.transform.iter().enumerate() {
                let has_external_input = !tx
                    .input_vars()
                    .unwrap_or_default()
                    .iter()
                    .all(|input_var| pipeline_vars.contains(&input_var.var));

                // Add output signals so we know they are available later
                for sig in &tx.output_signals() {
                    pipeline_vars.insert(Variable::new_signal(sig));
                }

                if has_external_input {
                    break;
                } else {
                    num_supported = i + 1
                }
            }

            if num_supported < data.transform.len() {
                // Perform split
                let parents_transforms = Vec::from(&data.transform[..num_supported]);
                let child_transforms = Vec::from(&data.transform[num_supported..]);

                // Compute new name for parent data
                let mut parent_name = data.name.clone();
                parent_name.insert_str(0, "_parent_");

                // Clone data for parent (with updated name)
                let mut parent_data = data.clone();
                parent_data.name = parent_name.clone();
                parent_data.transform = parents_transforms;

                // Save parent data node
                self.parent_url_data_nodes
                    .push((parent_data, Vec::from(scope)));

                // Update child data spec:
                //   - Same name
                //   - Add source of parent
                //   - Update remaining transforms
                data.source = Some(parent_name.clone());
                data.format = None;
                data.values = None;
                data.transform = child_transforms;
                data.on = None;
                data.url = None;
            }
        }
        Ok(())
    }
}
