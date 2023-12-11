use crate::planning::dependency_graph::{build_dependency_graph, toposort_dependency_graph};
use crate::proto::gen::tasks::VariableNamespace;
use crate::spec::chart::ChartSpec;
use crate::spec::values::StringOrSignalSpec;
use crate::spec::visitors::extract_inline_dataset;
use crate::task_graph::graph::ScopedVariable;
use petgraph::prelude::{EdgeRef, NodeIndex};
use petgraph::Direction;
use vegafusion_common::error::Result;

/// Optimize the server_spec by fusing datasets.
/// Currently, this function fuses parent datasets into their children when the parent dataset
/// does not include an aggregation or has exactly one child. This has the effect of pushing
/// transforms up through at least the first aggregation into the root dataset's transform
/// pipeline.
pub fn fuse_datasets(server_spec: &mut ChartSpec, do_not_fuse: &[ScopedVariable]) -> Result<()> {
    let (data_graph, _) = build_dependency_graph(server_spec, &Default::default())?;
    let nodes: Vec<NodeIndex> = toposort_dependency_graph(&data_graph)?;

    'outer: for node_index in &nodes {
        let (parent_var, _) = data_graph.node_weight(*node_index).unwrap();
        let Ok(parent_dataset) = server_spec
            .get_nested_data(&parent_var.1, &parent_var.0.name)
            .cloned()
        else {
            continue;
        };

        // Don't push down datasets that are required by client
        if do_not_fuse.contains(parent_var) {
            continue;
        }

        // Extract child variables
        let edges = data_graph.edges_directed(*node_index, Direction::Outgoing);
        let child_vars = edges
            .into_iter()
            .map(|edge| {
                let target_index = edge.target();
                let (child_var, _) = data_graph.node_weight(target_index).unwrap();
                child_var
            })
            .collect::<Vec<_>>();

        if child_vars.is_empty() {
            continue;
        }

        // Check if the url references an inline dataset. We assume that it's preferable to
        // push computation into the inline dataset's transform pipeline, whereas we don't want
        // to duplicate the loading of datasets from an https:// URL.
        let has_inline_dataset = if let Some(StringOrSignalSpec::String(s)) = &parent_dataset.url {
            extract_inline_dataset(s).is_some()
        } else {
            false
        };

        // Don't fuse down datasets with aggregate transforms and multiple children
        if (parent_dataset.has_aggregate() || !has_inline_dataset) && child_vars.len() > 1 {
            continue;
        }

        // Make sure all children are datasets
        let all_children_data = child_vars
            .iter()
            .all(|v| v.0.ns() == VariableNamespace::Data);
        if !all_children_data {
            continue;
        }

        // Make sure all children have this named dataset as source
        // (it's possible for a dataset's transform pipeline to reference this dataset, in which
        // case we can't fuse.)
        for child_var in &child_vars {
            let Ok(child_dataset) = server_spec.get_nested_data(&child_var.1, &child_var.0.name)
            else {
                continue 'outer;
            };
            if child_dataset.source.as_ref() != Some(&parent_dataset.name) {
                continue 'outer;
            }
        }

        // Extract child datasets
        for child_var in &child_vars {
            let Ok(child_dataset) =
                server_spec.get_nested_data_mut(&child_var.1, &child_var.0.name)
            else {
                continue 'outer;
            };
            parent_dataset.fuse_into(child_dataset)?;
        }

        // Convert parent dataset into a stub
        let Ok(parent_dataset) = server_spec.get_nested_data_mut(&parent_var.1, &parent_var.0.name)
        else {
            continue;
        };
        parent_dataset.transform = Default::default();
        parent_dataset.source = Default::default();
        parent_dataset.values = Default::default();
        parent_dataset.url = Default::default();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::planning::fuse::fuse_datasets;
    use crate::proto::gen::tasks::Variable;
    use crate::spec::chart::ChartSpec;
    use serde_json::json;

    #[test]
    fn test_simple_fuse() {
        let chart: ChartSpec = serde_json::from_value(json!(
            {
                "data": [
                    {
                        "name": "data_0",
                        "url": "vegafusion+dataset://foo",
                        "transform": [
                            {
                                "type": "filter",
                                "expr": "datum.A > 0"
                            }
                        ]
                    },
                    {
                        "name": "data_1",
                        "source": "data_0",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.A * 2",
                                "as": "B"
                            }
                        ]
                    },
                    {
                        "name": "data_2",
                        "source": "data_0",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.A * 3",
                                "as": "C"
                            }
                        ]
                    },
                    {
                        "name": "data_3",
                        "source": "data_1",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.A * 4",
                                "as": "C"
                            }
                        ]
                    },
                ]
            }
        ))
        .unwrap();

        // try fuse
        let mut new_chart = chart.clone();
        fuse_datasets(
            &mut new_chart,
            &[
                (Variable::new_data("data_2"), vec![]),
                (Variable::new_data("data_3"), vec![]),
            ],
        )
        .unwrap();
        let pretty_output = serde_json::to_string_pretty(&new_chart).unwrap();
        println!("{}", pretty_output);
        assert_eq!(
            pretty_output,
            r#"{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "data_0"
    },
    {
      "name": "data_1"
    },
    {
      "name": "data_2",
      "url": "vegafusion+dataset://foo",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.A > 0"
        },
        {
          "type": "formula",
          "expr": "datum.A * 3",
          "as": "C"
        }
      ]
    },
    {
      "name": "data_3",
      "url": "vegafusion+dataset://foo",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.A > 0"
        },
        {
          "type": "formula",
          "expr": "datum.A * 2",
          "as": "B"
        },
        {
          "type": "formula",
          "expr": "datum.A * 4",
          "as": "C"
        }
      ]
    }
  ]
}"#
        );
        // Check that fuse is prevented if parent dataset is needed by client
        let mut new_chart = chart.clone();
        fuse_datasets(&mut new_chart, &[(Variable::new_data("data_0"), vec![])]).unwrap();
        let pretty_output = serde_json::to_string_pretty(&new_chart).unwrap();
        println!("{}", pretty_output);
        assert_eq!(
            pretty_output,
            r#"{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "data_0",
      "url": "vegafusion+dataset://foo",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.A > 0"
        }
      ]
    },
    {
      "name": "data_1"
    },
    {
      "name": "data_2",
      "source": "data_0",
      "transform": [
        {
          "type": "formula",
          "expr": "datum.A * 3",
          "as": "C"
        }
      ]
    },
    {
      "name": "data_3",
      "source": "data_0",
      "transform": [
        {
          "type": "formula",
          "expr": "datum.A * 2",
          "as": "B"
        },
        {
          "type": "formula",
          "expr": "datum.A * 4",
          "as": "C"
        }
      ]
    }
  ]
}"#
        );
    }

    #[test]
    fn test_simple_fuse_with_aggregate() {
        let chart: ChartSpec = serde_json::from_value(json!(
            {
                "data": [
                    {
                        "name": "data_0",
                        "url": "vegafusion+dataset://foo",
                        "transform": [
                            {
                                "type": "filter",
                                "expr": "datum.A > 0"
                            }
                        ]
                    },
                    {
                        "name": "data_1",
                        "source": "data_0",
                        "transform": [
                            {
                                "type": "aggregate",
                                "fields": ["foo", "bar", "baz"],
                                "ops": ["valid", "sum", "median"],
                                "groupby": []
                            }
                        ]
                    },
                    {
                        "name": "data_2",
                        "source": "data_0",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.bar * 3",
                                "as": "C"
                            }
                        ]
                    },
                    {
                        "name": "data_3",
                        "source": "data_1",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.A * 4",
                                "as": "C"
                            }
                        ]
                    },
                    {
                        "name": "data_4",
                        "source": "data_1",
                        "transform": [
                            {
                                "type": "formula",
                                "expr": "datum.A * 5",
                                "as": "D"
                            }
                        ]
                    },
                ]
            }
        ))
        .unwrap();

        // try fuse
        let mut new_chart = chart.clone();
        fuse_datasets(
            &mut new_chart,
            &[
                (Variable::new_data("data_2"), vec![]),
                (Variable::new_data("data_3"), vec![]),
            ],
        )
        .unwrap();
        let pretty_output = serde_json::to_string_pretty(&new_chart).unwrap();
        println!("{}", pretty_output);
        assert_eq!(
            pretty_output,
            r#"{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "data_0"
    },
    {
      "name": "data_1",
      "url": "vegafusion+dataset://foo",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.A > 0"
        },
        {
          "type": "aggregate",
          "groupby": [],
          "fields": [
            "foo",
            "bar",
            "baz"
          ],
          "ops": [
            "valid",
            "sum",
            "median"
          ]
        }
      ]
    },
    {
      "name": "data_2",
      "url": "vegafusion+dataset://foo",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.A > 0"
        },
        {
          "type": "formula",
          "expr": "datum.bar * 3",
          "as": "C"
        }
      ]
    },
    {
      "name": "data_3",
      "source": "data_1",
      "transform": [
        {
          "type": "formula",
          "expr": "datum.A * 4",
          "as": "C"
        }
      ]
    },
    {
      "name": "data_4",
      "source": "data_1",
      "transform": [
        {
          "type": "formula",
          "expr": "datum.A * 5",
          "as": "D"
        }
      ]
    }
  ]
}"#
        );
    }
}
