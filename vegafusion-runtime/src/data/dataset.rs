use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::error::Result;
use vegafusion_dataframe::dataframe::DataFrame;

#[derive(Clone)]
pub enum VegaFusionDataset {
    Table { table: VegaFusionTable, hash: u64 },
    DataFrame(Arc<dyn DataFrame>),
}

impl VegaFusionDataset {
    pub fn fingerprint(&self) -> String {
        match self {
            VegaFusionDataset::Table { hash, .. } => hash.to_string(),
            VegaFusionDataset::DataFrame(df) => df.fingerprint().to_string(),
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
