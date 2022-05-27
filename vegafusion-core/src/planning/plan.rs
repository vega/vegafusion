/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::planning::extract::extract_server_data;
use crate::planning::optimize_server::split_data_url_nodes;
use crate::planning::projection_pushdown::projection_pushdown;
use crate::planning::split_domain_data::split_domain_data;
use crate::planning::stitch::{stitch_specs, CommPlan};
use crate::planning::stringify_local_datetimes::{
    stringify_local_datetimes, OutputLocalDatetimesConfig,
};
use crate::spec::chart::ChartSpec;

#[derive(Debug, Clone)]
pub struct PlannerConfig {
    pub split_domain_data: bool,
    pub split_url_data_nodes: bool,
    pub local_datetimes_config: OutputLocalDatetimesConfig,
    pub projection_pushdown: bool,
    pub extract_inline_data: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            split_domain_data: true,
            split_url_data_nodes: true,
            local_datetimes_config: Default::default(),
            projection_pushdown: true,
            extract_inline_data: false,
        }
    }
}

pub struct SpecPlan {
    pub server_spec: ChartSpec,
    pub client_spec: ChartSpec,
    pub comm_plan: CommPlan,
}

impl SpecPlan {
    pub fn try_new(full_spec: &ChartSpec, config: &PlannerConfig) -> Result<Self> {
        let mut client_spec = full_spec.clone();

        // Attempt to limit the columns produced by each dataset to only include those
        // that are actually used downstream
        if config.projection_pushdown {
            projection_pushdown(&mut client_spec)?;
        }

        // Split datasets that contain a mix of supported and unsupported transforms
        if config.split_domain_data {
            split_domain_data(&mut client_spec)?;
        }

        let mut task_scope = client_spec.to_task_scope()?;

        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope, config)?;
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;

        if config.split_url_data_nodes {
            split_data_url_nodes(&mut server_spec)?;
        }

        match &config.local_datetimes_config {
            OutputLocalDatetimesConfig::LocalNaiveString => {
                stringify_local_datetimes(&mut server_spec, &mut client_spec, &comm_plan, &None)?;
            }
            OutputLocalDatetimesConfig::TimezoneNaiveString(output_tz) => {
                stringify_local_datetimes(
                    &mut server_spec,
                    &mut client_spec,
                    &comm_plan,
                    &Some(output_tz.clone()),
                )?;
            }
            OutputLocalDatetimesConfig::UtcMillis => {
                // Leave local datetimes as UTC milliseconds
            }
        }

        Ok(Self {
            server_spec,
            client_spec,
            comm_plan,
        })
    }
}
