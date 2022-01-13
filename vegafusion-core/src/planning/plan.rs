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
use crate::planning::extract::extract_server_data;
use crate::planning::optimize_server::split_data_url_nodes;
use crate::planning::split_domain_data::split_domain_data;
use crate::planning::stitch::{stitch_specs, CommPlan};
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

        println!("{}", serde_json::to_string_pretty(&client_spec).unwrap());
        let mut task_scope = client_spec.to_task_scope()?;

        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope)?;
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec)?;

        split_data_url_nodes(&mut server_spec)?;

        Ok(Self {
            server_spec,
            client_spec,
            comm_plan,
        })
    }
}
