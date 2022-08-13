/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::data::table::VegaFusionTableUtils;
use datafusion::prelude::DataFrame;
use std::sync::Arc;

use vegafusion_core::error::Result;
use vegafusion_core::data::table::VegaFusionTable;
use crate::sql::dataframe::SqlDataFrame;

#[derive(Clone)]
pub enum VegaFusionDataset {
    Table { table: VegaFusionTable, hash: u64 },
    SqlDataFrame(SqlDataFrame)
}

impl VegaFusionDataset {
    pub fn fingerprint(&self) -> String {
        match self {
            VegaFusionDataset::Table { hash, .. } => hash.to_string(),
            VegaFusionDataset::SqlDataFrame(sql_df) => sql_df.fingerprint().to_string(),
        }
    }

    pub fn from_table_ipc_bytes(ipc_bytes: &[u8]) -> Result<Self> {
        // Hash ipc bytes
        let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());
        ipc_bytes.hash(&mut hasher);
        let hash = hasher.finish();
        let table = VegaFusionTable::from_ipc_bytes(ipc_bytes)?;
        Ok(Self::Table { table, hash })
    }
}
