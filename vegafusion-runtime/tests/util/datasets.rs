use reqwest_middleware::ClientWithMiddleware;
use serde_json::Value;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_runtime::data::tasks::make_request_client;

lazy_static! {
    pub static ref REQWEST_CLIENT: ClientWithMiddleware = make_request_client();
}

pub fn vega_json_dataset(name: &str) -> VegaFusionTable {
    // Remove trailing .json if present because we'll add it below
    let name = match name.strip_suffix(".json") {
        None => name,
        Some(name) => name,
    };

    // Fetch dataset from vega-datasets repository
    let body = reqwest::blocking::get(format!(
        "https://raw.githubusercontent.com/vega/vega-datasets/master/data/{name}.json"
    ))
    .unwrap()
    .text()
    .unwrap();
    let json_value: Value = serde_json::from_str(&body).unwrap();

    VegaFusionTable::from_json(&json_value).unwrap()
}

pub async fn vega_json_dataset_async(name: &str) -> VegaFusionTable {
    // Remove trailing .json if present because we'll add it below
    let name = match name.strip_suffix(".json") {
        None => name,
        Some(name) => name,
    };

    // Fetch dataset from vega-datasets repository
    let body = REQWEST_CLIENT
        .get(format!(
            "https://raw.githubusercontent.com/vega/vega-datasets/master/data/{name}.json"
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let json_value: Value = serde_json::from_str(&body).unwrap();

    VegaFusionTable::from_json(&json_value).unwrap()
}
