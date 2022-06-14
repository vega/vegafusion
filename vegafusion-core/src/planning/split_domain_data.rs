/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */

use crate::error::Result;
use crate::expression::escape::escape_field;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::scale::{
    ScaleDataReferenceSort, ScaleDataReferenceSortParameters, ScaleDataReferenceSpec,
    ScaleDomainSpec, ScaleSpec, ScaleTypeSpec,
};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use itertools::Itertools;
use std::collections::HashMap;

/// This optimization extracts the intensive data processing from scale.domain.data specifications
/// into dedicated datasets. Domain calculations can't be entirely evaluated on the server, but
/// this approach still allows the heavy data processing to happen on the server, and to avoid
/// serializing the full dataset to send to the client.
pub fn split_domain_data(
    spec: &mut ChartSpec,
) -> Result<HashMap<ScopedVariable, (ScopedVariable, String)>> {
    let task_scope = spec.to_task_scope()?;
    let mut visitor = SplitScaleDomainVisitor::new(&task_scope);
    spec.walk_mut(&mut visitor)?;
    for (scope, data) in visitor.new_datasets {
        if scope.is_empty() {
            spec.data.push(data);
        } else {
            let group = spec.get_nested_group_mut(scope.as_slice())?;
            group.data.push(data);
        }
    }

    Ok(visitor.domain_dataset_fields)
}

#[derive(Debug, Clone)]
pub struct SplitScaleDomainVisitor<'a> {
    pub task_scope: &'a TaskScope,
    pub new_datasets: Vec<(Vec<u32>, DataSpec)>,
    pub domain_dataset_fields: HashMap<ScopedVariable, (ScopedVariable, String)>,
}

impl<'a> SplitScaleDomainVisitor<'a> {
    pub fn new(task_scope: &'a TaskScope) -> Self {
        Self {
            new_datasets: Vec::new(),
            task_scope,
            domain_dataset_fields: Default::default(),
        }
    }
}

impl<'a> MutChartVisitor for SplitScaleDomainVisitor<'a> {
    fn visit_scale(&mut self, scale: &mut ScaleSpec, scope: &[u32]) -> Result<()> {
        let discrete_scale = scale.type_.clone().unwrap_or_default().is_discrete();
        if let Some(ScaleDomainSpec::FieldReference(field_ref)) = &scale.domain {
            let data_name = field_ref.data.clone();
            let data_var = (Variable::new_data(&data_name), Vec::from(scope));
            let field_name = &field_ref.field;

            // Validate whether we can do anything
            if field_name.contains('.') {
                // Nested fields not supported
                return Ok(());
            }

            // Build suffix using scope
            let mut scope_suffix = scope.iter().map(|s| s.to_string()).join("_");
            if !scope_suffix.is_empty() {
                scope_suffix.insert(0, '_');
            }

            let new_data_name = format!(
                "{}_{}_domain_{}{}",
                data_name, scale.name, field_name, scope_suffix
            );
            let new_data_var = (Variable::new_data(&new_data_name), Vec::from(scope));
            self.domain_dataset_fields
                .insert(new_data_var, (data_var, field_name.clone()));
            let (new_data, new_domain) = if discrete_scale {
                // Extract sort field and op
                let (sort_field, sort_op) = if let Some(ScaleDataReferenceSort::Parameters(
                    sort_params,
                )) = &field_ref.sort
                {
                    (sort_params.field.clone(), sort_params.op.clone())
                } else {
                    (None, None)
                };

                let new_data = if let Some(sort_op) = sort_op {
                    // Will sort by the result of an aggregation operation
                    let sort_field = sort_field.unwrap_or_else(|| field_name.clone());
                    serde_json::from_value(serde_json::json!(
                        {
                            "name": new_data_name,
                            "source": data_name,
                            "transform": [
                                {
                                    "type": "aggregate",
                                    "as": ["sort_field"],
                                    "groupby": [field_name],
                                    "ops": [sort_op],
                                    "fields": [sort_field]
                                }
                            ]
                       }
                    ))
                    .unwrap()
                } else {
                    // Will sort by the grouped field values
                    serde_json::from_value(serde_json::json!(
                        {
                            "name": new_data_name,
                            "source": data_name,
                            "transform": [
                                {
                                    "type": "aggregate",
                                    "as": [],
                                    "groupby": [field_name],
                                    "ops": [],
                                    "fields": []
                                }, {
                                    "type": "formula",
                                    "as": "sort_field",
                                    "expr": format!("datum['{}']", field_name)
                                }
                            ]
                       }
                    ))
                    .unwrap()
                };

                // Create new domain specification that uses the new dataset
                let sort = match &field_ref.sort {
                    Some(ScaleDataReferenceSort::Parameters(sort_params)) => Some(
                        ScaleDataReferenceSort::Parameters(ScaleDataReferenceSortParameters {
                            op: Some(AggregateOpSpec::Max),
                            field: Some("sort_field".to_string()),
                            ..sort_params.clone()
                        }),
                    ),
                    sort => sort.clone(),
                };

                let new_domain = ScaleDomainSpec::FieldReference(ScaleDataReferenceSpec {
                    data: new_data_name,
                    field: field_name.clone(),
                    sort,
                    extra: Default::default(),
                });

                (new_data, new_domain)
            } else if matches!(
                scale.type_.clone().unwrap_or_default(),
                ScaleTypeSpec::Linear
            ) {
                // Create derived dataset that performs the min/max calculations
                let new_data: DataSpec = serde_json::from_value(serde_json::json!(
                    {
                        "name": new_data_name,
                        "source": data_name,
                        "transform": [
                            {
                                "type": "formula",
                                "as": field_name,
                                "expr": format!("+datum['{}']", field_name)
                            }, {
                                "type": "aggregate",
                                "fields": [field_name, field_name],
                                "ops": ["min", "max"],
                                "as": ["min", "max"],
                                 "groupby": []
                            }
                        ]
                    }
                ))
                .unwrap();

                // Create new domain specification that uses the new dataset
                let new_domain: ScaleDomainSpec = serde_json::from_value(serde_json::json!([
                    {
                        "signal":
                            format!(
                                "(data(\"{}\")[0] || {{}}).min",
                                escape_field(&new_data_name)
                            )
                    },
                    {
                        "signal":
                            format!(
                                "(data(\"{}\")[0] || {{}}).max",
                                escape_field(&new_data_name)
                            )
                    }
                ]))
                .unwrap();

                (new_data, new_domain)
            } else {
                // Unsupported scale type
                return Ok(());
            };

            // Overwrite scale domain with new domain
            scale.domain = Some(new_domain);

            // Add new dataset at same scope as source dataset
            let resolved = self
                .task_scope
                .resolve_scope(&Variable::new_data(data_name.as_str()), scope)?;

            // Add new dataset at current scope
            self.new_datasets.push((resolved.scope, new_data))
        }
        Ok(())
    }
}
