/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::data::table::VegaFusionTableUtils;
use datafusion::prelude::DataFrame;
use std::sync::Arc;
use vegafusion_core::{data::dataset::VegaFusionDataset, error::Result};

pub trait VegaFusionDatasetUtils {
    fn to_dataframe(&self) -> Result<Arc<DataFrame>>;
}

impl VegaFusionDatasetUtils for VegaFusionDataset {
    fn to_dataframe(&self) -> Result<Arc<DataFrame>> {
        match self {
            VegaFusionDataset::Table { table, .. } => table.to_dataframe(),
        }
    }
}
