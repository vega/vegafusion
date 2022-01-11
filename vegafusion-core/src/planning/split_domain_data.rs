// VegaFusion
// Copyright (C) 2022, Jon Mease
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::error::Result;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::scale::{ScaleDataReferenceSort, ScaleDataReferenceSortParameters, ScaleDataReferenceSpec, ScaleDomainSpec, ScaleSpec};
use crate::spec::transform::aggregate::AggregateOpSpec;

/// This optimization extracts the intensive data processing from scale.domain.data specifications
/// into dedicated datasets. Domain calculations can't be entirely evaluated on the server, but
/// this approach still allows the heavy data processing to happen on the server, and to avoid
/// serializing the full dataset to send to the client.
pub fn split_domain_data(spec: &mut ChartSpec) -> Result<()> {
    let mut visitor = SplitUrlDataNodeVisitor::new();
    spec.walk_mut(&mut visitor)?;
    for (scope, data) in visitor.new_datasets {
        if scope.is_empty() {
            spec.data.push(data);
        } else {
            let group = spec.get_nested_group_mut(scope.as_slice())?;
            group.data.push(data);
        }
    }

    Ok(())
}


#[derive(Debug, Clone, Default)]
pub struct SplitUrlDataNodeVisitor {
    pub new_datasets: Vec<(Vec<u32>, DataSpec)>
}


impl SplitUrlDataNodeVisitor {
    pub fn new() -> Self {
        Self { new_datasets: Vec::new() }
    }
}


impl MutChartVisitor for SplitUrlDataNodeVisitor {
    fn visit_scale(&mut self, scale: &mut ScaleSpec, scope: &[u32]) -> Result<()> {
        let discrete_scale = scale.type_.clone().unwrap_or_default().is_discrete();
        if let Some(domain) = &scale.domain {
            match domain {
                ScaleDomainSpec::FieldReference(field_ref) => {
                    let data_name = &field_ref.data;
                    let field_name = &field_ref.field;

                    let new_data_name = format!("{}_{}_domain_{}", data_name, scale.name, field_name);
                    let (new_data, new_domain) = if discrete_scale {

                        // Extract sort field and op
                        let (sort_field, sort_op) = if let Some(ScaleDataReferenceSort::Parameters(sort_params)) = &field_ref.sort {
                            (sort_params.field.clone(), sort_params.op.clone())
                        } else {
                            (None, None)
                        };
                        let sort_field = sort_field.unwrap_or_else(|| field_name.clone());
                        let sort_op = sort_op.unwrap_or(AggregateOpSpec::Count);

                        // Create derived dataset that performs the aggregation
                        let new_data: DataSpec = serde_json::from_value(serde_json::json!(
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
                        )).unwrap();

                        // Create new domain specification that uses the new dataset
                        let sort = match &field_ref.sort {
                            Some(ScaleDataReferenceSort::Parameters(sort_params)) => {
                                Some(ScaleDataReferenceSort::Parameters(ScaleDataReferenceSortParameters {
                                    op: Some(AggregateOpSpec::Max),
                                    field: Some("sort_field".to_string()),
                                    ..sort_params.clone()
                                }))
                            }
                            sort => sort.clone()
                        };

                        let new_domain = ScaleDomainSpec::FieldReference(ScaleDataReferenceSpec {
                            data: new_data_name,
                            field: field_name.clone(),
                            sort,
                            extra: Default::default()
                        });

                        (new_data, new_domain)
                    } else {
                        // Create derived dataset that performs the min/max calculations
                        let new_data: DataSpec = serde_json::from_value(serde_json::json!(
                            {
                                "name": new_data_name,
                                "source": data_name,
                                "transform": [
                                   {
                                       "type": "aggregate",
                                       "fields": [field_name, field_name],
                                       "ops": ["min", "max"],
                                       "as": ["min", "max"],
                                       "groupby": []
                                   }
                                ]
                            }
                        )).unwrap();

                        // Create new domain specification that uses the new dataset
                        let new_domain: ScaleDomainSpec = serde_json::from_value(serde_json::json!(
                            [
                                {"signal": format!("data('{}')[0].min", new_data_name)},
                                {"signal": format!("data('{}')[0].max", new_data_name)}
                            ]
                        )).unwrap();

                        (new_data, new_domain)
                    };

                    // Overwrite scale domain with new domain
                    scale.domain = Some(new_domain);

                    // Add new dataset at current scope
                    self.new_datasets.push((Vec::from(scope), new_data))
                }
                // TODO: handle FieldsReference
                // ScaleDomainSpec::FieldsReference(_) => {}
                _ => {}
            }
        }
        Ok(())
    }
}

