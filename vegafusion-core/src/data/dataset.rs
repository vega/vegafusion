use crate::error::Result;
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
            VegaFusionDataset::Plan { plan } => ahash::RandomState::with_seed(123)
                .hash_one(plan)
                .to_string(),
        }
    }

    pub fn from_table(table: VegaFusionTable, hash: Option<u64>) -> Result<Self> {
        let hash = hash.unwrap_or_else(|| table.get_hash());
        Ok(Self::Table { table, hash })
    }

    pub fn from_table_ipc_bytes(ipc_bytes: &[u8]) -> Result<Self> {
        // Hash ipc bytes
        let hash = ahash::RandomState::with_seed(123).hash_one(ipc_bytes);
        let table = VegaFusionTable::from_ipc_bytes(ipc_bytes)?;
        Ok(Self::Table { table, hash })
    }

    pub fn from_plan(plan: LogicalPlan) -> Self {
        Self::Plan { plan }
    }
}
