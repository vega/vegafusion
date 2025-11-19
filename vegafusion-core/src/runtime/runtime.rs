use std::{any::Any, collections::HashMap, sync::Arc};

use crate::proto::gen::pretransform::pre_transform_values_warning::WarningType as ValuesWarningType;
use crate::runtime::{NoOpPlanExecutor, PlanExecutor};
use crate::task_graph::task_value::MaterializedTaskValue;
use crate::{
    data::dataset::VegaFusionDataset,
    planning::{
        apply_pre_transform::apply_pre_transform_datasets,
        destringify_selection_datetimes::destringify_selection_datetimes,
        plan::{PlannerConfig, SpecPlan},
        watch::{ExportUpdate, ExportUpdateArrow, ExportUpdateNamespace},
    },
    proto::gen::{
        pretransform::{
            pre_transform_extract_warning, PlannerWarning, PreTransformExtractOpts,
            PreTransformExtractWarning, PreTransformLogicalPlanOpts,
            PreTransformLogicalPlanWarning, PreTransformRowLimitWarning, PreTransformSpecOpts,
            PreTransformSpecWarning, PreTransformValuesOpts, PreTransformValuesWarning,
        },
        tasks::{NodeValueIndex, TaskGraph, TzConfig, VariableNamespace},
    },
    spec::{chart::ChartSpec, values::MissingNullOrValue},
    task_graph::{graph::ScopedVariable, task_value::NamedTaskValue},
};
use async_trait::async_trait;
use futures::future::try_join_all;
use vegafusion_common::{
    data::table::VegaFusionTable,
    error::{Result, ResultWithContext, VegaFusionError},
};

#[derive(Clone, Debug)]
pub struct PreTransformExtractTable {
    pub name: String,
    pub scope: Vec<u32>,
    pub table: VegaFusionTable,
}

#[async_trait]
pub trait VegaFusionRuntimeTrait: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn plan_executor(&self) -> Arc<dyn PlanExecutor> {
        Arc::new(NoOpPlanExecutor::default())
    }

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> Result<Vec<NamedTaskValue>>;

    async fn materialize_export_updates(
        &self,
        export_updates: Vec<ExportUpdate>,
    ) -> Result<Vec<ExportUpdateArrow>> {
        let executor = self.plan_executor();
        try_join_all(export_updates.into_iter().map(|eu| {
            let exec = executor.clone();
            async move {
                let value = eu.value.to_materialized(exec).await?;
                Ok(ExportUpdateArrow {
                    namespace: eu.namespace,
                    name: eu.name,
                    scope: eu.scope,
                    value,
                })
            }
        }))
        .await
    }

    async fn pre_transform_spec_plan(
        &self,
        spec: &ChartSpec,
        local_tz: &str,
        default_input_tz: &Option<String>,
        preserve_interactivity: bool,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        keep_variables: Vec<ScopedVariable>,
    ) -> Result<(SpecPlan, Vec<ExportUpdate>)> {
        // Create spec plan
        let plan = SpecPlan::try_new(
            spec,
            &PlannerConfig::pre_transformed_spec_config(preserve_interactivity, keep_variables),
        )?;

        // Extract inline dataset fingerprints
        let dataset_fingerprints = inline_datasets
            .iter()
            .map(|(k, ds)| (k.clone(), ds.fingerprint()))
            .collect::<HashMap<_, _>>();

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: local_tz.to_string(),
            default_input_tz: default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &dataset_fingerprints)
            .unwrap();
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        // Gather values of server-to-client values
        let mut init = Vec::new();
        let task_graph = Arc::new(task_graph);
        let indices: Vec<NodeValueIndex> = plan
            .comm_plan
            .server_to_client
            .iter()
            .filter_map(|var| task_graph_mapping.get(var).cloned())
            .collect();

        let response_values = self
            .query_request(task_graph.clone(), &indices, inline_datasets)
            .await
            .with_context(|| "Failed to query node values")?;

        for (var, response_value) in plan.comm_plan.server_to_client.iter().zip(response_values) {
            init.push(ExportUpdate {
                namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
                name: var.0.name.clone(),
                scope: var.1.clone(),
                value: response_value.value,
            });
        }
        Ok((plan, init))
    }

    async fn pre_transform_spec(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformSpecOpts,
    ) -> Result<(ChartSpec, Vec<PreTransformSpecWarning>)> {
        let input_spec = spec;

        let keep_variables: Vec<ScopedVariable> = options
            .keep_variables
            .clone()
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();
        let (plan, init) = self
            .pre_transform_spec_plan(
                spec,
                &options.local_tz,
                &options.default_input_tz,
                options.preserve_interactivity,
                inline_datasets,
                keep_variables,
            )
            .await?;

        let init_arrow = self.materialize_export_updates(init).await?;

        apply_pre_transform_datasets(input_spec, &plan, init_arrow, options.row_limit)
    }

    async fn pre_transform_extract(
        &self,
        spec: &ChartSpec,
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformExtractOpts,
    ) -> Result<(
        ChartSpec,
        Vec<PreTransformExtractTable>,
        Vec<PreTransformExtractWarning>,
    )> {
        let input_spec = spec;
        let keep_variables: Vec<ScopedVariable> = options
            .keep_variables
            .clone()
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();

        let (plan, init) = self
            .pre_transform_spec_plan(
                spec,
                &options.local_tz,
                &options.default_input_tz,
                options.preserve_interactivity,
                inline_datasets,
                keep_variables,
            )
            .await?;
        let init_arrow = self.materialize_export_updates(init).await?;

        // Update client spec with server values
        let mut spec = plan.client_spec.clone();
        let mut datasets: Vec<PreTransformExtractTable> = Vec::new();
        let extract_threshold = options.extract_threshold as usize;

        for export_update in init_arrow {
            let scope = export_update.scope.clone();
            let name = export_update.name.as_str();
            match export_update.namespace {
                ExportUpdateNamespace::Signal => {
                    // Always inline signal values
                    let signal = spec.get_nested_signal_mut(&scope, name)?;
                    signal.value = MissingNullOrValue::Value(export_update.value.to_json()?);
                }
                ExportUpdateNamespace::Data => {
                    let data = spec.get_nested_data_mut(&scope, name)?;

                    // If the input dataset includes inline values and no transforms,
                    // copy the input JSON directly to avoid the case where round-tripping
                    // through Arrow homogenizes mixed type arrays.
                    // E.g. round tripping may turn [1, "two"] into ["1", "two"]
                    let input_values =
                        input_spec
                            .get_nested_data(&scope, name)
                            .ok()
                            .and_then(|data| {
                                if data.transform.is_empty() {
                                    data.values.clone()
                                } else {
                                    None
                                }
                            });
                    if let Some(input_values) = input_values {
                        // Set inline value
                        data.values = Some(input_values);
                    } else if let MaterializedTaskValue::Table(table) = export_update.value {
                        if table.num_rows() <= extract_threshold {
                            // Inline small tables
                            data.values = Some(table.to_json()?);
                        } else {
                            // Extract non-small tables
                            datasets.push(PreTransformExtractTable {
                                name: export_update.name,
                                scope: export_update.scope,
                                table,
                            });
                        }
                    } else {
                        return Err(VegaFusionError::internal(
                            "Expected Data TaskValue to be an Table",
                        ));
                    }
                }
            }
        }

        // Destringify datetime strings in selection store datasets
        destringify_selection_datetimes(&mut spec)?;

        // Build warnings
        let mut warnings: Vec<PreTransformExtractWarning> = Vec::new();

        // Add planner warnings
        for planner_warning in &plan.warnings {
            warnings.push(PreTransformExtractWarning {
                warning_type: Some(pre_transform_extract_warning::WarningType::Planner(
                    PlannerWarning {
                        message: planner_warning.message(),
                    },
                )),
            });
        }

        Ok((spec, datasets, warnings))
    }

    async fn pre_transform_values(
        &self,
        spec: &ChartSpec,
        variables: &[ScopedVariable],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
        options: &PreTransformValuesOpts,
    ) -> Result<(Vec<MaterializedTaskValue>, Vec<PreTransformValuesWarning>)> {
        // Check that requested variables exist and collect indices
        for var in variables {
            let scope = var.1.as_slice();
            let variable = var.0.clone();
            let name = variable.name.clone();
            let namespace = variable.clone().ns();

            match namespace {
                VariableNamespace::Signal => {
                    if spec.get_nested_signal(scope, &name).is_err() {
                        return Err(VegaFusionError::pre_transform(format!(
                            "No signal named {} with scope {:?}",
                            name, scope
                        )));
                    }
                }
                VariableNamespace::Data => {
                    if spec.get_nested_data(scope, &name).is_err() {
                        return Err(VegaFusionError::pre_transform(format!(
                            "No dataset named {} with scope {:?}",
                            name, scope
                        )));
                    }
                }
                VariableNamespace::Scale => {
                    return Err(VegaFusionError::pre_transform(format!(
                        "pre_transform_values does not support scale variable {:?}",
                        variable
                    )))
                }
            }
        }

        // Make sure planner keeps the requested variables, event
        // if they are not used elsewhere in the spec
        let keep_variables = Vec::from(variables);

        // Create spec plan
        let plan = SpecPlan::try_new(
            spec,
            &PlannerConfig {
                stringify_local_datetimes: false,
                extract_inline_data: true,
                split_domain_data: false,
                projection_pushdown: false,
                allow_client_to_server_comms: true,
                keep_variables,
                ..Default::default()
            },
        )?;

        // Extract inline dataset fingerprints
        let dataset_fingerprints = inline_datasets
            .iter()
            .map(|(k, ds)| (k.clone(), ds.fingerprint()))
            .collect::<HashMap<_, _>>();

        // Create task graph for server spec
        let tz_config = TzConfig {
            local_tz: options.local_tz.to_string(),
            default_input_tz: options.default_input_tz.clone(),
        };
        let task_scope = plan.server_spec.to_task_scope().unwrap();
        let tasks = plan
            .server_spec
            .to_tasks(&tz_config, &dataset_fingerprints)?;
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        let mut warnings: Vec<PreTransformValuesWarning> = Vec::new();

        // Add planner warnings
        for planner_warning in &plan.warnings {
            warnings.push(PreTransformValuesWarning {
                warning_type: Some(ValuesWarningType::Planner(PlannerWarning {
                    message: planner_warning.message(),
                })),
            });
        }

        // Collect node indices for variables
        let indices: Vec<_> = variables
            .iter()
            .map(|var| {
                if let Some(index) = task_graph_mapping.get(&(var.0.clone(), var.1.clone())) {
                    Ok(*index)
                } else {
                    Err(VegaFusionError::pre_transform(format!(
                        "Requested variable {var:?}\n requires transforms or signal \
                            expressions that are not yet supported"
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // perform query
        let named_task_values = self
            .query_request(Arc::new(task_graph.clone()), &indices, inline_datasets)
            .await?;

        // Collect values and handle row limit in parallel
        let row_limit = options.row_limit.map(|l| l as usize);
        let plan_executor = self.plan_executor();

        let materialized_futures = named_task_values.into_iter().map(|named_task_value| {
            let plan_executor = plan_executor.clone();
            let row_limit = row_limit;
            async move {
                let value = named_task_value.value;
                let variable = named_task_value.variable;

                let materialized_value = value.to_materialized(plan_executor).await?;

                // Apply row_limit and collect warnings
                let (final_value, warning) =
                    if let (Some(row_limit), MaterializedTaskValue::Table(table)) =
                        (row_limit, &materialized_value)
                    {
                        let warning = PreTransformValuesWarning {
                            warning_type: Some(ValuesWarningType::RowLimit(
                                PreTransformRowLimitWarning {
                                    datasets: vec![variable],
                                },
                            )),
                        };
                        (
                            MaterializedTaskValue::Table(table.head(row_limit)),
                            Some(warning),
                        )
                    } else {
                        (materialized_value, None)
                    };

                Ok::<_, VegaFusionError>((final_value, warning))
            }
        });

        let materialized_results = try_join_all(materialized_futures).await?;
        let mut task_values = Vec::new();
        for (value, warning) in materialized_results {
            task_values.push(value);
            if let Some(warning) = warning {
                warnings.push(warning);
            }
        }

        Ok((task_values, warnings))
    }

    async fn pre_transform_logical_plan(
        &self,
        spec: &ChartSpec,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        options: &PreTransformLogicalPlanOpts,
    ) -> Result<(
        ChartSpec,
        Vec<ExportUpdate>,
        Vec<PreTransformLogicalPlanWarning>,
    )> {
        let keep_variables: Vec<ScopedVariable> = options
            .keep_variables
            .clone()
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();
        let (plan, export_updates) = self
            .pre_transform_spec_plan(
                spec,
                &options.local_tz,
                &options.default_input_tz,
                options.preserve_interactivity,
                &inline_datasets,
                keep_variables,
            )
            .await?;

        let warnings: Vec<PreTransformLogicalPlanWarning> = plan
            .warnings
            .iter()
            .map(|planner_warning| PreTransformLogicalPlanWarning {
                warning_type: Some(
                    crate::proto::gen::pretransform::pre_transform_logical_plan_warning::WarningType::Planner(
                        PlannerWarning {
                            message: planner_warning.message(),
                        },
                    ),
                ),
            })
            .collect();

        Ok((plan.client_spec, export_updates, warnings))
    }
}
