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
use crate::planning::split_domain_data::split_domain_data;
use crate::planning::stitch::{stitch_specs, CommPlan};
use crate::planning::stringify_local_datetimes::stringify_local_datetimes;
use crate::spec::chart::ChartSpec;

pub struct SpecPlan {
    pub server_spec: ChartSpec,
    pub client_spec: ChartSpec,
    pub comm_plan: CommPlan,
}

impl SpecPlan {
    pub fn try_new(full_spec: &ChartSpec) -> Result<Self> {
        let mut client_spec = full_spec.clone();
        split_domain_data(&mut client_spec)?;

        let mut task_scope = client_spec.to_task_scope()?;

        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope)?;
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;

        split_data_url_nodes(&mut server_spec)?;

        stringify_local_datetimes(
            &mut server_spec, &mut client_spec, &comm_plan
        )?;

        Ok(Self {
            server_spec,
            client_spec,
            comm_plan,
        })
    }
}
