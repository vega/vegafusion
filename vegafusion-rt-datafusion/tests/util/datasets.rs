use serde_json::Value;
use vegafusion_rt_datafusion::data::table::VegaFusionTable;

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

    VegaFusionTable::from_json(json_value, 1024).unwrap()
}
