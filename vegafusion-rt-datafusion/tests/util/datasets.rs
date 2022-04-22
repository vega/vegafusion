/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use serde_json::Value;
use vegafusion_core::data::table::VegaFusionTable;

pub fn vega_json_dataset(name: &str) -> VegaFusionTable {
    // Remove trailing .json if present because we'll add it below
    let name = match name.strip_suffix(".json") {
        None => name,
        Some(name) => name,
    };

    // Fetch dataset from vega-datasets repository
    let body = reqwest::blocking::get(format!(
        "https://raw.githubusercontent.com/vega/vega-datasets/master/data/{}.json",
        name
    ))
    .unwrap()
    .text()
    .unwrap();
    let json_value: Value = serde_json::from_str(&body).unwrap();

    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}

pub async fn vega_json_dataset_async(name: &str) -> VegaFusionTable {
    // Remove trailing .json if present because we'll add it below
    let name = match name.strip_suffix(".json") {
        None => name,
        Some(name) => name,
    };

    // Fetch dataset from vega-datasets repository
    let body = reqwest::get(format!(
        "https://raw.githubusercontent.com/vega/vega-datasets/master/data/{}.json",
        name
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();
    let json_value: Value = serde_json::from_str(&body).unwrap();

    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}
