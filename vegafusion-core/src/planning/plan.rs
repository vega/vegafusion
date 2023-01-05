use crate::error::Result;
use crate::planning::extract::extract_server_data;
use crate::planning::optimize_server::split_data_url_nodes;
use crate::planning::projection_pushdown::projection_pushdown;
use crate::planning::split_domain_data::split_domain_data;
use crate::planning::stitch::{stitch_specs, CommPlan};
use crate::planning::stringify_local_datetimes::stringify_local_datetimes;
use crate::planning::unsupported_data_warning::add_unsupported_data_warnings;
use crate::spec::chart::ChartSpec;
use crate::task_graph::graph::ScopedVariable;

#[derive(Clone, Debug)]
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
    pub allow_client_to_server_comms: bool,
    pub client_only_vars: Vec<ScopedVariable>,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            split_domain_data: true,
            split_url_data_nodes: true,
            stringify_local_datetimes: false,
            projection_pushdown: true,
            extract_inline_data: false,
            allow_client_to_server_comms: true,
            client_only_vars: Default::default(),
        }
    }
}

#[derive(Debug)]
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

        // Attempt to limit the columns produced by each dataset to only include those
        // that are actually used downstream
        if config.projection_pushdown {
            projection_pushdown(&mut client_spec)?;
        }

        let mut task_scope = client_spec.to_task_scope()?;
        let input_client_spec = client_spec.clone();
        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope, config)?;
        let mut comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;

        if !config.allow_client_to_server_comms && !comm_plan.client_to_server.is_empty() {
            // Client to server comms are not allowed and the initial planning
            // pass included them. re-plan without
            let mut config = config.clone();
            config
                .client_only_vars
                .extend(comm_plan.client_to_server.clone());

            client_spec = input_client_spec;
            server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config)?;
            comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;
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
