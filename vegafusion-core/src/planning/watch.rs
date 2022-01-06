/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::error::Result;
use crate::error::VegaFusionError;
use crate::planning::stitch::CommPlan;
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use crate::task_graph::graph::ScopedVariable;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::TryFrom;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchNamespace {
    Signal,
    Data,
}

impl TryFrom<VariableNamespace> for WatchNamespace {
    type Error = VegaFusionError;

    fn try_from(value: VariableNamespace) -> Result<Self> {
        match value {
            VariableNamespace::Signal => Ok(Self::Signal),
            VariableNamespace::Data => Ok(Self::Data),
            _ => Err(VegaFusionError::internal("Scale namespace not supported")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Watch {
    pub namespace: WatchNamespace,
    pub name: String,
    pub scope: Vec<u32>,
}

impl Watch {
    pub fn to_scoped_variable(&self) -> ScopedVariable {
        (
            match self.namespace {
                WatchNamespace::Signal => Variable::new_signal(&self.name),
                WatchNamespace::Data => Variable::new_data(&self.name),
            },
            self.scope.clone(),
        )
    }
}

impl TryFrom<ScopedVariable> for Watch {
    type Error = VegaFusionError;

    fn try_from(value: ScopedVariable) -> Result<Self> {
        let tmp = value.0.namespace();
        let tmp = WatchNamespace::try_from(tmp)?;
        Ok(Self {
            namespace: tmp,
            name: value.0.name.clone(),
            scope: value.1,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchPlan {
    pub server_to_client: Vec<Watch>,
    pub client_to_server: Vec<Watch>,
}

impl From<CommPlan> for WatchPlan {
    fn from(value: CommPlan) -> Self {
        Self {
            server_to_client: value
                .server_to_client
                .into_iter()
                .map(|scoped_var| Watch::try_from(scoped_var).unwrap())
                .sorted()
                .collect(),
            client_to_server: value
                .client_to_server
                .into_iter()
                .map(|scoped_var| Watch::try_from(scoped_var).unwrap())
                .sorted()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchValue {
    pub watch: Watch,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchValues {
    pub values: Vec<WatchValue>,
}
