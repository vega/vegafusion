use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::mark::MarkSpec;
use crate::spec::transform::facet::FacetTransformSpec;
use crate::spec::transform::TransformSpec;
use crate::task_graph::scope::TaskScope;
use vegafusion_common::error::Result;
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::joinaggregate::JoinAggregateTransformSpec;
use crate::spec::values::Field;

pub fn extract_facet_data(
    server_spec: &mut ChartSpec,
    client_spec: &mut ChartSpec,
    full_task_scope: &mut TaskScope,
    config: &PlannerConfig,
) -> Result<()> {
    let mut visitor = ExtractFacetDataVisitor::try_new(server_spec, full_task_scope, config)?;
    client_spec.walk_mut(&mut visitor)?;
    Ok(())
}

#[derive(Debug)]
pub struct ExtractFacetDataVisitor<'a> {
    pub server_spec: &'a mut ChartSpec,
    pub server_scope: TaskScope,
    pub full_task_scope: &'a mut TaskScope,
    pub config: &'a PlannerConfig,
}

impl<'a> ExtractFacetDataVisitor<'a> {
    pub fn try_new(
        server_spec: &'a mut ChartSpec,
        full_task_scope: &'a mut TaskScope,
        config: &'a PlannerConfig,
    ) -> Result<Self> {
        let server_scope = server_spec.to_task_scope()?;
        Ok(Self {
            server_spec,
            full_task_scope,
            server_scope,
            config,
        })
    }
}

impl<'a> MutChartVisitor for ExtractFacetDataVisitor<'a> {
    fn visit_group_mark(&mut self, mark: &mut MarkSpec, scope: &[u32]) -> Result<()> {
        let Some(from) = &mut mark.from else { return Ok(()) };
        let Some(facet) = &mut from.facet else { return Ok(()) };
        let facet_groupby = facet.groupby.clone();
        if facet_groupby.is_empty() || facet_groupby.len() > 2 {
            // Our facet transform only supports one or two groupby fields
            return Ok(());
        }

        for mark in &mark.marks {
            if let Some(from) = &mark.from {
                if let Some(mark_from_data) = &from.data {
                    if mark_from_data == &facet.name {
                        // facet dataset is used directly by a mark, so we can't pre-transform it
                        return Ok(())
                    }
                }
            }
        }

        if facet.aggregate.is_some() {
            // Aggregate not supported yet
            return Ok(())
        }

        // Find child dataset with facet as source
        // let facet_child_dataset = None;
        let mut facet_child_datasets = Vec::new();
        for fcds in &mut mark.data {
            let Some(source) = &fcds.source else { continue };
            if source == &facet.name {
                facet_child_datasets.push(fcds);
            }
        }
        if facet_child_datasets.len() != 1 {
            // Only extract if facet dataset is used as the input to a single dataset
            return Ok(())
        }
        let facet_child_dataset = facet_child_datasets.pop().unwrap();

        let mut facet_child_transforms = facet_child_dataset.transform.clone();

        if facet_child_transforms.is_empty() {
            // No facet transforms, nothing to do
            return Ok(());
        }

        // Check that facet input data is available on the server
        let Ok(input_dataset) = self.server_scope.resolve_scope(
            &Variable::new_data(&facet.data), scope
        ) else { return Ok(())};

        // Check that all transforms are supported
        for tx in &facet_child_transforms {
            if !tx.supported() {
                return Ok(());
            }
            for v in tx.input_vars()? {
                if self.server_scope.resolve_scope(&v.var, scope).is_err() {
                    return Ok(());
                }
            }
        }

        // Now:
        //  - All facet transforms are supported
        //  - All facet transform inputs are available on the server
        //  - The facet input dataset is available on the server
        // So we can perform extraction
        let facet_data_name = format!("_facet_{}_{}", facet.data, facet.name);

        // Add dataset that performs faceting
        let group_data = if input_dataset.scope.is_empty() {
            &mut self.server_spec.data
        } else {
            let Ok(data_group) = self.server_spec.get_nested_group_mut(
                input_dataset.scope.as_slice()
            ) else { return Ok(() )};
            &mut data_group.data
        };

        group_data.push(DataSpec {
            name: facet_data_name.clone(),
            source: Some(facet.data.clone()),
            transform: vec![TransformSpec::Facet(FacetTransformSpec {
                groupby: facet_groupby,
                transform: facet_child_dataset.transform.clone(),
                extra: Default::default(),
            })],
            url: None,
            format: None,
            values: None,
            on: None,
            extra: Default::default(),
        });

        // Add new dataset to full task scope
        self.full_task_scope.add_variable(
            &Variable::new_data(&facet_data_name),
            input_dataset.scope.as_slice(),
        )?;

        // Update facet to point to the new dataset
        facet.data = facet_data_name;

        // Clear facet aggregate
        facet.aggregate = None;

        // Clear facet child dataset's transforms
        // (since we moved them to the facet dataset on the server)
        facet_child_dataset.transform.clear();

        Ok(())
    }
}
