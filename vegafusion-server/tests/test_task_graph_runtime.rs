use assert_cmd::prelude::*;
use std::time::Duration;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::proto::gen::services::query_result::Response;
use vegafusion_core::proto::gen::services::vega_fusion_runtime_client::VegaFusionRuntimeClient;
use vegafusion_core::proto::gen::services::{query_request, QueryRequest};
use vegafusion_core::proto::gen::tasks::{
    NodeValueIndex, TaskGraph, TaskGraphValueRequest, TzConfig, VariableNamespace,
};
use vegafusion_core::spec::chart::ChartSpec; // Add methods on commands

#[tokio::test(flavor = "multi_thread")]
async fn try_it_from_spec() {
    let chart: ChartSpec = serde_json::from_str(
        r##"{
  "signals": [
    {
      "name": "url",
      "value": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/penguins.json"
    }
  ],
  "data": [
    {
      "name": "url_datasetA",
      "url": {"signal": "url"}
    },
    {
      "name": "datasetA",
      "source": "url_datasetA",
      "transform": [
        {
          "type": "extent",
          "signal": "my_extent",
          "field": "Beak Length (mm)"
        },
        {
          "type": "collect",
          "sort": {"field": "Beak Length (mm)"}
        }
      ]
    }
  ]
}
"##,
    )
    .unwrap();

    let local_tz = "America/New_York";
    let tz_config = TzConfig {
        local_tz: local_tz.to_string(),
        default_input_tz: None,
    };
    let task_scope = chart.to_task_scope().unwrap();
    let tasks = chart.to_tasks(&tz_config, &Default::default()).unwrap();

    let graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let request = QueryRequest {
        request: Some(query_request::Request::TaskGraphValues(
            TaskGraphValueRequest {
                task_graph: Some(graph),
                indices: vec![NodeValueIndex::new(2, Some(0))],
            },
        )),
    };

    let mut bin = std::process::Command::cargo_bin("vegafusion-server")
        .expect("Failed to build vegafusion-server");
    let cmd = bin.args(["--port", "50059"]);

    let mut proc = cmd.spawn().expect("Failed to spawn vegafusion-server");
    std::thread::sleep(Duration::from_millis(2000));

    let mut client = VegaFusionRuntimeClient::connect("http://127.0.0.1:50059")
        .await
        .expect("Failed to connect to gRPC server");
    let response = client.task_graph_query(request).await.unwrap();

    let query_result = response.into_inner();
    match query_result.response.unwrap() {
        Response::Error(error) => {
            panic!("Error: {error:?}")
        }
        Response::TaskGraphValues(values_response) => {
            let response_values = values_response.deserialize().unwrap();
            println!("Result: {response_values:#?}");
            assert_eq!(response_values.len(), 1);
            let (var, scope, value) = &response_values[0];

            assert_eq!(var.name.as_str(), "my_extent");
            assert_eq!(var.namespace(), VariableNamespace::Signal);
            assert_eq!(scope, &Vec::<u32>::new());
            assert_eq!(
                &value.as_scalar().unwrap().to_f64x2().unwrap(),
                &[32.1, 59.6],
            )
        }
    }
    proc.kill().ok();
}
