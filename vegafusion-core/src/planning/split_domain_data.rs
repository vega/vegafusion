use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::scale::{
    ScaleDataReferenceOrSignalSpec, ScaleDataReferenceSort, ScaleDataReferenceSortParameters,
    ScaleDomainSpec, ScaleFieldReferenceSpec, ScaleFieldsReferencesSpec, ScaleSpec, ScaleTypeSpec,
};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use itertools::Itertools;
use std::collections::HashMap;
use vegafusion_common::error::Result;
use vegafusion_common::escape::escape_field;

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
        if let Some(domain) = scale.domain.clone() {
            match domain {
                ScaleDomainSpec::FieldReference(field_ref) => {
                    self.split_field_reference_domain(scale, scope, &field_ref)?;
                }
                ScaleDomainSpec::FieldsReferences(fields_ref) => {
                    self.split_fields_reference_domain(scale, scope, &fields_ref)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl<'a> SplitScaleDomainVisitor<'a> {
    fn split_fields_reference_domain(
        &mut self,
        scale: &mut ScaleSpec,
        scope: &[u32],
        fields_ref: &ScaleFieldsReferencesSpec,
    ) -> Result<()> {
        let discrete_scale = scale.type_.clone().unwrap_or_default().is_discrete();
        let (new_datasets, new_dataset_scope, new_domain) = if discrete_scale {
            // Extract sort field and op
            let (sort_field, sort_op) = match &fields_ref.sort {
                Some(ScaleDataReferenceSort::Parameters(sort_params)) => {
                    (sort_params.field.clone(), sort_params.op.clone())
                }
                _ => (None, None),
            };

            // Iterate over data fields
            let mut new_datasets = Vec::new();
            let mut new_dataset_scope = Vec::new();
            let mut new_fields = Vec::new();
            for (field_index, field_ref) in fields_ref.fields.iter().enumerate() {
                if let ScaleDataReferenceOrSignalSpec::Reference(field_ref) = field_ref {
                    let field_name = &field_ref.field;
                    let data_name = field_ref.data.clone();
                    let scope_suffix = Self::build_scope_suffix(scope);

                    let new_data_name = format!(
                        "{}_{}_domain_{}{}_{}",
                        data_name, scale.name, field_name, scope_suffix, field_index
                    );

                    let new_data = Self::make_discrete_domain_data(
                        &data_name,
                        &new_data_name,
                        field_name,
                        sort_field.clone(),
                        sort_op.clone(),
                    )?;
                    new_datasets.push(new_data);

                    // Compute new domain field
                    let mut new_field_ref = field_ref.clone();
                    new_field_ref.data = new_data_name.clone();
                    new_fields.push(new_field_ref);

                    // Compute scope for the original referenced dataset
                    let resolved = self
                        .task_scope
                        .resolve_scope(&Variable::new_data(data_name.as_str()), scope)?;
                    new_dataset_scope.push(resolved.scope);
                }
            }

            // Create new domain specification that uses the new fields
            let sort = match &fields_ref.sort {
                Some(ScaleDataReferenceSort::Parameters(sort_params)) => Some(
                    ScaleDataReferenceSort::Parameters(ScaleDataReferenceSortParameters {
                        op: Some(AggregateOpSpec::Max),
                        field: Some("sort_field".to_string()),
                        ..sort_params.clone()
                    }),
                ),
                sort => sort.clone(),
            };

            let new_domain = ScaleDomainSpec::FieldsReferences(ScaleFieldsReferencesSpec {
                fields: new_fields
                    .into_iter()
                    .map(ScaleDataReferenceOrSignalSpec::Reference)
                    .collect(),
                sort,
                extra: Default::default(),
            });

            (new_datasets, new_dataset_scope, new_domain)
        } else {
            // Scale type not supported
            return Ok(());
        };

        // Overwrite scale domain with new domain
        scale.domain = Some(new_domain);

        for (new_dataset, scope) in new_datasets.into_iter().zip(new_dataset_scope) {
            // Add new dataset at current scope
            self.new_datasets.push((scope, new_dataset));
        }

        Ok(())
    }

    fn split_field_reference_domain(
        &mut self,
        scale: &mut ScaleSpec,
        scope: &[u32],
        field_ref: &ScaleFieldReferenceSpec,
    ) -> Result<()> {
        let discrete_scale = scale.type_.clone().unwrap_or_default().is_discrete();
        let data_name = field_ref.data.clone();
        let data_var = (Variable::new_data(&data_name), Vec::from(scope));
        let field_name = &field_ref.field;

        // Validate whether we can do anything
        if field_name.contains('.') {
            // Nested fields not supported
            return Ok(());
        }

        let scope_suffix = Self::build_scope_suffix(scope);

        let new_data_name = format!(
            "{}_{}_domain_{}{}",
            data_name, scale.name, field_name, scope_suffix
        );
        let new_data_var = (Variable::new_data(&new_data_name), Vec::from(scope));
        self.domain_dataset_fields
            .insert(new_data_var, (data_var, field_name.clone()));

        let (new_data, new_domain) = if discrete_scale {
            // Extract sort field and op
            let (sort_field, sort_op) =
                if let Some(ScaleDataReferenceSort::Parameters(sort_params)) = &field_ref.sort {
                    (sort_params.field.clone(), sort_params.op.clone())
                } else {
                    (None, None)
                };

            let new_data = Self::make_discrete_domain_data(
                &data_name,
                &new_data_name,
                field_name,
                sort_field,
                sort_op,
            )?;

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

            let new_domain = ScaleDomainSpec::FieldReference(ScaleFieldReferenceSpec {
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
                            "expr": format!("+datum['{field_name}']")
                        }, {
                            "type": "aggregate",
                            "fields": [field_name, field_name],
                            "ops": ["min", "max"],
                            "as": ["min", "max"],
                             "groupby": []
                        }
                    ]
                }
            ))?;

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
            ]))?;

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
        self.new_datasets.push((resolved.scope, new_data));
        Ok(())
    }

    fn build_scope_suffix(scope: &[u32]) -> String {
        // Build suffix using scope
        let mut scope_suffix = scope.iter().map(|s| s.to_string()).join("_");
        if !scope_suffix.is_empty() {
            scope_suffix.insert(0, '_');
        }
        scope_suffix
    }

    /// Make a Vega dataset that computes the discrete values of an input dataset with
    /// an optional sorting field
    fn make_discrete_domain_data(
        data_name: &str,
        new_data_name: &str,
        field_name: &String,
        sort_field: Option<String>,
        sort_op: Option<AggregateOpSpec>,
    ) -> Result<DataSpec> {
        Ok(if let Some(sort_op) = sort_op {
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
            ))?
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
                            "expr": format!("datum['{field_name}']")
                        }
                    ]
               }
            ))?
        })
    }
}
