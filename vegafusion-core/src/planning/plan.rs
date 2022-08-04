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
use crate::planning::stringify_local_datetimes::stringify_local_datetimes;
use crate::spec::chart::ChartSpec;

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
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            split_domain_data: true,
            split_url_data_nodes: true,
            stringify_local_datetimes: false,
            projection_pushdown: true,
            extract_inline_data: false,
        }
    }
}

pub struct SpecPlan {
    pub server_spec: ChartSpec,
    pub client_spec: ChartSpec,
    pub comm_plan: CommPlan,
    pub warnings: Vec<PlannerWarnings>,
}

impl SpecPlan {
    pub fn try_new(full_spec: &ChartSpec, config: &PlannerConfig) -> Result<Self> {
        let mut warnings: Vec<PlannerWarnings> = Vec::new();

        let mut client_spec = full_spec.clone();

        // Attempt to limit the columns produced by each dataset to only include those
        // that are actually used downstream
        if config.projection_pushdown {
            projection_pushdown(&mut client_spec)?;
        }

        let domain_dataset_fields = if config.split_domain_data {
            split_domain_data(&mut client_spec)?
        } else {
            Default::default()
        };

        let mut task_scope = client_spec.to_task_scope()?;

        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &mut warnings, config)?;
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;

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
