use crate::error::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;

#[derive(Clone)]
pub enum VegaFusionDataset {
    Table { table: VegaFusionTable, hash: u64 },
    Plan { plan: LogicalPlan },
}

impl VegaFusionDataset {
    pub fn fingerprint(&self) -> String {
        match self {
            VegaFusionDataset::Table { hash, .. } => hash.to_string(),
            VegaFusionDataset::Plan { plan } => {
                let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());
                plan.hash(&mut hasher);
                hasher.finish().to_string()
            }
        }
    }

    pub fn from_table(table: VegaFusionTable, hash: Option<u64>) -> Result<Self> {
        let hash = hash.unwrap_or_else(|| table.get_hash());
        Ok(Self::Table { table, hash })
    }

    pub fn from_table_ipc_bytes(ipc_bytes: &[u8]) -> Result<Self> {
        // Hash ipc bytes
        let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());
        ipc_bytes.hash(&mut hasher);
        let hash = hasher.finish();
        let table = VegaFusionTable::from_ipc_bytes(ipc_bytes)?;
        Ok(Self::Table { table, hash })
    }

    pub fn from_plan(plan: LogicalPlan) -> Self {
        Self::Plan { plan }
    }
}
