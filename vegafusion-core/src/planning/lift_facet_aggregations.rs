use crate::planning::dependency_graph::{build_dependency_graph, DependencyGraph};
use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::mark::MarkSpec;
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::joinaggregate::JoinAggregateTransformSpec;
use crate::spec::transform::TransformSpec;
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use petgraph::prelude::NodeIndex;
use petgraph::Direction;
use std::collections::HashMap;
use vegafusion_common::error::Result;

/// Optimization that lifts aggregation transforms inside facet definitions into top-level
/// datasets that VegaFusion can evaluate
pub fn lift_facet_aggregations(chart_spec: &mut ChartSpec, config: &PlannerConfig) -> Result<()> {
    let (graph, node_indexes) = build_dependency_graph(chart_spec, config)?;
    let mut visitor =
        ExtractFacetAggregationsVisitor::new(chart_spec.to_task_scope()?, graph, node_indexes)?;
    chart_spec.walk_mut(&mut visitor)?;
    visitor.insert_lifted_datasets(chart_spec)?;
    Ok(())
}

pub struct ExtractFacetAggregationsVisitor {
    pub task_scope: TaskScope,
    pub graph: DependencyGraph,
    pub node_indexes: HashMap<ScopedVariable, NodeIndex>,
    pub new_datasets: HashMap<Vec<u32>, Vec<DataSpec>>,
    pub counter: u32,
}

impl ExtractFacetAggregationsVisitor {
    pub fn new(
        task_scope: TaskScope,
        graph: DependencyGraph,
        node_indexes: HashMap<ScopedVariable, NodeIndex>,
    ) -> Result<Self> {
        Ok(Self {
            task_scope,
            graph,
            node_indexes,
            new_datasets: Default::default(),
            counter: 0,
        })
    }

    /// Add lifted datasets to the provided ChartSpec.
    /// This must be called after chart.walk_mut(&mut visitor)
    pub fn insert_lifted_datasets(&self, chart_spec: &mut ChartSpec) -> Result<()> {
        for (scope, new_datasets_for_scope) in &self.new_datasets {
            let datasets = if scope.is_empty() {
                &mut chart_spec.data
            } else {
                &mut chart_spec.get_nested_group_mut(scope.as_slice())?.data
            };
            for new_dataset in new_datasets_for_scope {
                datasets.push(new_dataset.clone())
            }
        }
        Ok(())
    }
}

impl MutChartVisitor for ExtractFacetAggregationsVisitor {
    fn visit_group_mark(&mut self, mark: &mut MarkSpec, scope: &[u32]) -> Result<()> {
        let Some(from) = &mut mark.from else {
            return Ok(());
        };
        let Some(facet) = &mut from.facet else {
            return Ok(());
        };

        // Check for child datasets
        let facet_dataset_var: ScopedVariable = (Variable::new_data(&facet.name), Vec::from(scope));
        let Some(facet_dataset_idx) = self.node_indexes.get(&facet_dataset_var) else {
            return Ok(());
        };
        let edges = self
            .graph
            .edges_directed(*facet_dataset_idx, Direction::Outgoing);
        let edges_vec = edges.into_iter().collect::<Vec<_>>();
        if edges_vec.len() != 1 {
            // We don't have exactly one child dataset so we cannot lift
            return Ok(());
        }

        // Collect datasets that are immediate children of the facet dataset
        let mut child_datasets = mark
            .data
            .iter_mut()
            .filter(|d| d.source.as_ref() == Some(&facet.name))
            .collect::<Vec<_>>();

        if child_datasets.len() != 1 {
            // Child dataset isn't located in this facet's dataset.
            // I don't think this shouldn't happen, but bail out in case
            return Ok(());
        }

        let child_dataset = &mut child_datasets[0];
        let Some(TransformSpec::Aggregate(mut agg)) = child_dataset.transform.get(0).cloned()
        else {
            // dataset does not have a aggregate transform as the first transform, nothing to lift
            return Ok(());
        };

        // Add facet groupby fields as aggregate transform groupby fields
        let facet_groupby_fields: Vec<Field> = facet
            .groupby
            .clone()
            .unwrap_or_default()
            .to_vec()
            .into_iter()
            .map(Field::String)
            .collect();

        agg.groupby.extend(facet_groupby_fields.clone());

        let mut lifted_transforms: Vec<TransformSpec> = Vec::new();

        // When the facet defines an aggregation, we need to perform it with a joinaggregate
        // prior to the lifted aggregation.
        //
        // Leave `cross` field as-is
        if let Some(facet_aggregate) = &mut facet.aggregate {
            if facet_aggregate.fields.is_some()
                && facet_aggregate.ops.is_some()
                && facet_aggregate.as_.is_some()
            {
                // Add joinaggregate transform that performs the facet's aggregation using the same
                // grouping columns as the facet
                lifted_transforms.push(TransformSpec::JoinAggregate(JoinAggregateTransformSpec {
                    groupby: Some(facet_groupby_fields),
                    fields: facet_aggregate.fields.clone().unwrap(),
                    ops: facet_aggregate.ops.clone().unwrap(),
                    as_: facet_aggregate.as_.clone(),
                    extra: Default::default(),
                }));

                // Add aggregations to the lifted aggregate transform that pass through the
                // fields that the joinaggregate above calculates
                let mut new_fields = agg.fields.clone().unwrap_or_default();
                let mut new_ops = agg.ops.clone().unwrap_or_default();
                let mut new_as = agg.as_.clone().unwrap_or_default();

                new_fields.extend(
                    facet_aggregate
                        .as_
                        .clone()
                        .unwrap()
                        .into_iter()
                        .map(|s| s.map(Field::String)),
                );
                // Use min aggregate to pass through single unique value
                new_ops.extend(facet_aggregate.ops.iter().map(|_| AggregateOpSpec::Min));
                new_as.extend(facet_aggregate.as_.clone().unwrap());

                agg.fields = Some(new_fields);
                agg.ops = Some(new_ops);
                agg.as_ = Some(new_as);

                // Update facet aggregate to pass through the fields compute in joinaggregate
                facet_aggregate.fields = Some(
                    facet_aggregate
                        .as_
                        .clone()
                        .unwrap()
                        .into_iter()
                        .map(|s| s.map(Field::String))
                        .collect(),
                );
                facet_aggregate.ops = Some(
                    facet_aggregate
                        .ops
                        .iter()
                        .map(|_| AggregateOpSpec::Min)
                        .collect(),
                );
            } else if facet_aggregate.fields.is_some()
                || facet_aggregate.ops.is_some()
                || facet_aggregate.as_.is_some()
            {
                // Not all of fields, ops, and as are defined so skip lifting
                return Ok(());
            }
        }

        // Add lifted aggregate transform, potentially after the joinaggregate transform
        lifted_transforms.push(TransformSpec::Aggregate(agg));

        // Create facet dataset name and increment counter to keep names unique even if the same
        // source dataset is used in multiple facets
        let facet_dataset_name = format!("{}_facet_{}{}", facet.data, facet.name, self.counter);
        self.counter += 1;

        // Create new dataset that should be added to the spec at the resolved scope
        let new_dataset = DataSpec {
            name: facet_dataset_name.clone(),
            source: Some(facet.data.clone()),
            transform: lifted_transforms,
            url: None,
            format: None,
            values: None,
            on: None,
            extra: Default::default(),
        };

        // Save new dataset at the same scope as the original input dataset
        let Ok(resolved) = self
            .task_scope
            .resolve_scope(&Variable::new_data(&facet.data), &scope[..scope.len() - 1])
        else {
            return Ok(());
        };

        self.new_datasets
            .entry(resolved.scope.clone())
            .or_default()
            .push(new_dataset);

        // Remove leading aggregate transform from child dataset
        child_dataset.transform.remove(0);

        // Rename source dataset in facet
        facet.data = facet_dataset_name;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::planning::lift_facet_aggregations::lift_facet_aggregations;
    use crate::spec::chart::ChartSpec;
    use serde_json::json;

    #[test]
    fn test_simple_facet_lift() {
        let mut chart_spec: ChartSpec = serde_json::from_value(json!(
            {
              "$schema": "https://vega.github.io/schema/vega/v5.json",
              "data": [
                {
                  "name": "source_0",
                  "url": "https://cdn.jsdelivr.net/npm/vega-datasets@v1.29.0/data/barley.json",
                  "format": {"type": "json"}
                }
              ],
              "marks": [
                {
                  "name": "cell",
                  "type": "group",
                  "from": {
                    "facet": {"name": "facet", "data": "source_0", "groupby": ["site"]}
                  },
                  "sort": {"field": ["datum[\"site\"]"], "order": ["ascending"]},
                  "data": [
                    {
                      "source": "facet",
                      "name": "data_0",
                      "transform": [
                        {
                          "type": "aggregate",
                          "groupby": ["year"],
                          "ops": ["ci0", "ci1", "mean", "mean"],
                          "fields": ["yield", "yield", "yield", "yield"],
                          "as": ["lower_yield", "upper_yield", "center_yield", "mean_yield"]
                        }
                      ]
                    },
                    {
                      "name": "data_1",
                      "source": "data_0",
                      "transform": [
                        {
                          "type": "filter",
                          "expr": "isValid(datum[\"lower_yield\"]) && isFinite(+datum[\"lower_yield\"])"
                        }
                      ]
                    },
                    {
                      "name": "data_2",
                      "source": "data_0",
                      "transform": [
                        {
                          "type": "filter",
                          "expr": "isValid(datum[\"mean_yield\"]) && isFinite(+datum[\"mean_yield\"])"
                        }
                      ]
                    }
                  ],
                }
              ],
            }
        )).unwrap();

        lift_facet_aggregations(&mut chart_spec, &Default::default()).unwrap();
        let pretty_spec = serde_json::to_string_pretty(&chart_spec).unwrap();
        println!("{pretty_spec}");
        assert_eq!(
            pretty_spec,
            r#"{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "source_0",
      "url": "https://cdn.jsdelivr.net/npm/vega-datasets@v1.29.0/data/barley.json",
      "format": {
        "type": "json"
      }
    },
    {
      "name": "source_0_facet_facet0",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": [
            "year",
            "site"
          ],
          "fields": [
            "yield",
            "yield",
            "yield",
            "yield"
          ],
          "ops": [
            "ci0",
            "ci1",
            "mean",
            "mean"
          ],
          "as": [
            "lower_yield",
            "upper_yield",
            "center_yield",
            "mean_yield"
          ]
        }
      ]
    }
  ],
  "marks": [
    {
      "type": "group",
      "name": "cell",
      "from": {
        "facet": {
          "data": "source_0_facet_facet0",
          "name": "facet",
          "groupby": [
            "site"
          ]
        }
      },
      "sort": {
        "field": [
          "datum[\"site\"]"
        ],
        "order": [
          "ascending"
        ]
      },
      "data": [
        {
          "name": "data_0",
          "source": "facet"
        },
        {
          "name": "data_1",
          "source": "data_0",
          "transform": [
            {
              "type": "filter",
              "expr": "isValid(datum[\"lower_yield\"]) && isFinite(+datum[\"lower_yield\"])"
            }
          ]
        },
        {
          "name": "data_2",
          "source": "data_0",
          "transform": [
            {
              "type": "filter",
              "expr": "isValid(datum[\"mean_yield\"]) && isFinite(+datum[\"mean_yield\"])"
            }
          ]
        }
      ]
    }
  ]
}"#
        )
    }
}
