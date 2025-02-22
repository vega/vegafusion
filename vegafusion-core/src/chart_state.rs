use crate::{
    data::dataset::VegaFusionDataset,
    planning::{
        apply_pre_transform::apply_pre_transform_datasets,
        plan::SpecPlan,
        stitch::CommPlan,
        watch::{ExportUpdateArrow, ExportUpdateJSON, ExportUpdateNamespace},
    },
    proto::gen::{
        pretransform::PreTransformSpecWarning,
        tasks::{NodeValueIndex, TaskGraph, TzConfig, Variable, VariableNamespace},
    },
    runtime::VegaFusionRuntimeTrait,
    spec::chart::ChartSpec,
    task_graph::{graph::ScopedVariable, task_value::TaskValue},
};
use datafusion_common::ScalarValue;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use vegafusion_common::{
    data::{scalar::ScalarValueHelpers, table::VegaFusionTable},
    error::{Result, ResultWithContext, VegaFusionError},
};

#[derive(Clone, Debug)]
pub struct ChartStateOpts {
    pub tz_config: TzConfig,
    pub row_limit: Option<u32>,
}

impl Default for ChartStateOpts {
    fn default() -> Self {
        Self {
            tz_config: TzConfig {
                local_tz: "UTC".to_string(),
                default_input_tz: None,
            },
            row_limit: None,
        }
    }
}

#[derive(Clone)]
pub struct ChartState {
    input_spec: ChartSpec,
    transformed_spec: ChartSpec,
    plan: SpecPlan,
    inline_datasets: HashMap<String, VegaFusionDataset>,
    task_graph: Arc<Mutex<TaskGraph>>,
    task_graph_mapping: Arc<HashMap<ScopedVariable, NodeValueIndex>>,
    server_to_client_value_indices: Arc<HashSet<NodeValueIndex>>,
    warnings: Vec<PreTransformSpecWarning>,
}

impl ChartState {
    pub async fn try_new(
        runtime: &dyn VegaFusionRuntimeTrait,
        spec: ChartSpec,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        opts: ChartStateOpts,
    ) -> Result<Self> {
        let dataset_fingerprints = inline_datasets
            .iter()
            .map(|(k, ds)| (k.clone(), ds.fingerprint()))
            .collect::<HashMap<_, _>>();

        let plan = SpecPlan::try_new(&spec, &Default::default())?;

        let task_scope = plan
            .server_spec
            .to_task_scope()
            .with_context(|| "Failed to create task scope for server spec")?;
        let tasks = plan
            .server_spec
            .to_tasks(&opts.tz_config, &dataset_fingerprints)
            .unwrap();
        let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();
        let server_to_client_value_indices: Arc<HashSet<_>> = Arc::new(
            plan.comm_plan
                .server_to_client
                .iter()
                .map(|scoped_var| *task_graph_mapping.get(scoped_var).unwrap())
                .collect(),
        );

        // Gather values of server-to-client values using query_request
        let indices: Vec<NodeValueIndex> = plan
            .comm_plan
            .server_to_client
            .iter()
            .map(|var| *task_graph_mapping.get(var).unwrap())
            .collect();

        let response_task_values = runtime
            .query_request(Arc::new(task_graph.clone()), &indices, &inline_datasets)
            .await?;

        let mut init = Vec::new();
        for response_value in response_task_values {
            let variable = response_value.variable;

            let scope = response_value.scope;
            let value = response_value.value;

            init.push(ExportUpdateArrow {
                namespace: ExportUpdateNamespace::try_from(variable.ns()).unwrap(),
                name: variable.name.clone(),
                scope,
                value,
            });
        }

        let (transformed_spec, warnings) =
            apply_pre_transform_datasets(&spec, &plan, init, opts.row_limit)?;

        Ok(Self {
            input_spec: spec,
            transformed_spec,
            plan,
            inline_datasets,
            task_graph: Arc::new(Mutex::new(task_graph)),
            task_graph_mapping: Arc::new(task_graph_mapping),
            server_to_client_value_indices,
            warnings,
        })
    }

    pub async fn update(
        &self,
        runtime: &dyn VegaFusionRuntimeTrait,
        updates: Vec<ExportUpdateJSON>,
    ) -> Result<Vec<ExportUpdateJSON>> {
        // Scope the mutex guard to ensure it's dropped before the async call
        let (indices, cloned_task_graph) = {
            let mut task_graph = self.task_graph.lock().map_err(|err| {
                VegaFusionError::internal(format!("Failed to acquire task graph lock: {:?}", err))
            })?;
            let server_to_client = self.server_to_client_value_indices.clone();
            let mut indices: Vec<NodeValueIndex> = Vec::new();

            for export_update in &updates {
                let var = match export_update.namespace {
                    ExportUpdateNamespace::Signal => Variable::new_signal(&export_update.name),
                    ExportUpdateNamespace::Data => Variable::new_data(&export_update.name),
                };
                let scoped_var: ScopedVariable = (var, export_update.scope.clone());
                let node_value_index = *self
                    .task_graph_mapping
                    .get(&scoped_var)
                    .with_context(|| format!("No task graph node found for {scoped_var:?}"))?;

                let value = match export_update.namespace {
                    ExportUpdateNamespace::Signal => {
                        TaskValue::Scalar(ScalarValue::from_json(&export_update.value)?)
                    }
                    ExportUpdateNamespace::Data => {
                        TaskValue::Table(VegaFusionTable::from_json(&export_update.value)?)
                    }
                };

                indices
                    .extend(task_graph.update_value(node_value_index.node_index as usize, value)?);
            }

            // Filter to update nodes in the comm plan
            let indices: Vec<_> = indices
                .iter()
                .filter(|&node| server_to_client.contains(node))
                .cloned()
                .collect();

            // Clone the task graph while we still have the lock
            let cloned_task_graph = task_graph.clone();

            // Return both values we need
            (indices, cloned_task_graph)
        }; // MutexGuard is dropped here

        // Now we can safely make the async call
        let response_task_values = runtime
            .query_request(
                Arc::new(cloned_task_graph),
                indices.as_slice(),
                &self.inline_datasets,
            )
            .await?;

        let mut response_updates = response_task_values
            .into_iter()
            .map(|response_value| {
                let variable = response_value.variable;
                let scope = response_value.scope;
                let value = response_value.value;

                Ok(ExportUpdateJSON {
                    namespace: match variable.ns() {
                        VariableNamespace::Signal => ExportUpdateNamespace::Signal,
                        VariableNamespace::Data => ExportUpdateNamespace::Data,
                        VariableNamespace::Scale => {
                            return Err(VegaFusionError::internal("Unexpected scale variable"))
                        }
                    },
                    name: variable.name.clone(),
                    scope: scope.clone(),
                    value: value.to_json()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Sort for deterministic ordering
        response_updates.sort_by_key(|update| update.name.clone());

        Ok(response_updates)
    }

    pub fn get_input_spec(&self) -> &ChartSpec {
        &self.input_spec
    }

    pub fn get_server_spec(&self) -> &ChartSpec {
        &self.plan.server_spec
    }

    pub fn get_client_spec(&self) -> &ChartSpec {
        &self.plan.client_spec
    }

    pub fn get_transformed_spec(&self) -> &ChartSpec {
        &self.transformed_spec
    }

    pub fn get_comm_plan(&self) -> &CommPlan {
        &self.plan.comm_plan
    }

    pub fn get_warnings(&self) -> &Vec<PreTransformSpecWarning> {
        &self.warnings
    }
}
