use crate::error::Result;
use crate::planning::extract::extract_server_data;
use crate::planning::fuse::fuse_datasets;
use crate::planning::lift_facet_aggregations::lift_facet_aggregations;
use crate::planning::optimize_server::split_data_url_nodes;
use crate::planning::projection_pushdown::projection_pushdown;
use crate::planning::split_domain_data::split_domain_data;
use crate::planning::stitch::{stitch_specs, CommPlan};
use crate::planning::stringify_local_datetimes::stringify_local_datetimes;
use crate::planning::unsupported_data_warning::add_unsupported_data_warnings;
use crate::proto::gen::pretransform::{
    pre_transform_spec_warning::WarningType, PlannerWarning, PreTransformSpecWarning,
};
use crate::spec::chart::ChartSpec;
use crate::task_graph::graph::ScopedVariable;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct PreTransformSpecWarningSpec {
    #[serde(rename = "type")]
    pub typ: String,
    pub message: String,
}

impl From<&PreTransformSpecWarning> for PreTransformSpecWarningSpec {
    fn from(warning: &PreTransformSpecWarning) -> Self {
        match warning.warning_type.as_ref().unwrap() {
            WarningType::RowLimit(_) => {
                PreTransformSpecWarningSpec {
                    typ: "RowLimitExceeded".to_string(),
                    message: "Some datasets in resulting Vega specification have been truncated to the provided row limit".to_string()
                }
            }
            WarningType::BrokenInteractivity(_) => {
                PreTransformSpecWarningSpec {
                    typ: "BrokenInteractivity".to_string(),
                    message: "Some interactive features may have been broken in the resulting Vega specification".to_string()
                }
            }
            WarningType::Unsupported(_) => {
                PreTransformSpecWarningSpec {
                    typ: "Unsupported".to_string(),
                    message: "Unable to pre-transform any datasets in the Vega specification".to_string()
                }
            }
            WarningType::Planner(warning) => {
                PreTransformSpecWarningSpec {
                    typ: "Planner".to_string(),
                    message: warning.message.clone()
                }
            }
        }
    }
}

impl From<&PlannerWarning> for PreTransformSpecWarning {
    fn from(value: &PlannerWarning) -> Self {
        PreTransformSpecWarning {
            warning_type: Some(WarningType::Planner(PlannerWarning {
                message: value.message.clone(),
            })),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlannerWarnings {
    StringifyDatetimeMixedUsage(String),
    UnsupportedTransforms(String),
}

impl PlannerWarnings {
    pub fn message(&self) -> String {
        match &self {
            PlannerWarnings::StringifyDatetimeMixedUsage(message) => message.clone(),
            PlannerWarnings::UnsupportedTransforms(message) => message.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub split_domain_data: bool,
    pub split_url_data_nodes: bool,
    pub stringify_local_datetimes: bool,
    pub projection_pushdown: bool,
    pub extract_inline_data: bool,
    pub extract_server_data: bool,
    pub allow_client_to_server_comms: bool,
    pub fuse_datasets: bool,
    pub lift_facet_aggregations: bool,
    pub client_only_vars: Vec<ScopedVariable>,
    pub keep_variables: Vec<ScopedVariable>,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            split_domain_data: true,
            split_url_data_nodes: true,
            stringify_local_datetimes: false,
            projection_pushdown: true,
            extract_inline_data: false,
            extract_server_data: true,
            allow_client_to_server_comms: true,
            fuse_datasets: true,
            lift_facet_aggregations: true,
            client_only_vars: Default::default(),
            keep_variables: Default::default(),
        }
    }
}

impl PlannerConfig {
    pub fn pre_transformed_spec_config(
        preserve_interactivity: bool,
        keep_variables: Vec<ScopedVariable>,
    ) -> PlannerConfig {
        PlannerConfig {
            stringify_local_datetimes: true,
            extract_inline_data: true,
            allow_client_to_server_comms: !preserve_interactivity,
            keep_variables,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpecPlan {
    pub server_spec: ChartSpec,
    pub client_spec: ChartSpec,
    pub comm_plan: CommPlan,
    pub warnings: Vec<PlannerWarnings>,
}

impl SpecPlan {
    pub fn try_new(full_spec: &ChartSpec, config: &PlannerConfig) -> Result<Self> {
        let mut warnings: Vec<PlannerWarnings> = Vec::new();

        // Collect warnings associated with unsupported datasets
        add_unsupported_data_warnings(full_spec, config, &mut warnings)?;

        let mut client_spec = full_spec.clone();

        // Push domain data calculations to the server
        let domain_dataset_fields = if config.split_domain_data {
            split_domain_data(&mut client_spec)?
        } else {
            Default::default()
        };

        // Lift aggregation transforms out of facets so they can be evaluated on the server
        if config.lift_facet_aggregations {
            lift_facet_aggregations(&mut client_spec, config)?;
        }

        // Attempt to limit the columns produced by each dataset to only include those
        // that are actually used downstream
        if config.projection_pushdown {
            projection_pushdown(&mut client_spec)?;
        }

        if !config.extract_server_data {
            // Return empty server spec and empty comm plan
            Ok(Self {
                server_spec: Default::default(),
                client_spec,
                comm_plan: Default::default(),
                warnings,
            })
        } else {
            let mut task_scope = client_spec.to_task_scope()?;
            let input_client_spec = client_spec.clone();
            let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope, config)?;
            let mut comm_plan = stitch_specs(
                &task_scope,
                &mut server_spec,
                &mut client_spec,
                config.keep_variables.as_slice(),
            )?;

            if !config.allow_client_to_server_comms && !comm_plan.client_to_server.is_empty() {
                // Client to server comms are not allowed and the initial planning
                // pass included them. re-plan without
                let mut config = config.clone();
                config
                    .client_only_vars
                    .extend(comm_plan.client_to_server.clone());

                client_spec = input_client_spec;
                server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config)?;
                comm_plan = stitch_specs(
                    &task_scope,
                    &mut server_spec,
                    &mut client_spec,
                    config.keep_variables.as_slice(),
                )?;
            }

            if config.fuse_datasets {
                let mut do_not_fuse = config.keep_variables.clone();
                do_not_fuse.extend(comm_plan.server_to_client.clone());
                fuse_datasets(&mut server_spec, do_not_fuse.as_slice())?;
            }

            if config.split_url_data_nodes {
                split_data_url_nodes(&mut server_spec)?;
            }

            if config.stringify_local_datetimes {
                stringify_local_datetimes(
                    &mut server_spec,
                    &mut client_spec,
                    &comm_plan,
                    &domain_dataset_fields,
                )?;
            }

            Ok(Self {
                server_spec,
                client_spec,
                comm_plan,
                warnings,
            })
        }
    }
}
