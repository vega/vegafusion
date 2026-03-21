use serde_json::json;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_core::proto::gen::services::query_result::Response;
use vegafusion_core::proto::gen::services::vega_fusion_runtime_client::VegaFusionRuntimeClient;
use vegafusion_core::proto::gen::services::{query_request, QueryRequest};
use vegafusion_core::proto::gen::tasks::{
    TaskGraph, TaskGraphValueRequest, TzConfig, Variable, VariableNamespace,
};
use vegafusion_core::spec::chart::ChartSpec;

struct ServerProcess {
    child: Child,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pick_unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_json_rows(dir: &Path, name: &str, values: &[f64]) -> PathBuf {
    let path = dir.join(name);
    let rows: Vec<_> = values.iter().map(|value| json!({ "x": value })).collect();
    fs::write(&path, serde_json::to_string(&rows).unwrap()).unwrap();
    path
}

fn extent_spec(url: serde_json::Value) -> ChartSpec {
    serde_json::from_value(json!({
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": "source",
                "url": url,
                "format": {"type": "json"},
            },
            {
                "name": "derived",
                "source": "source",
                "transform": [
                    {
                        "type": "extent",
                        "signal": "my_extent",
                        "field": "x",
                    }
                ],
            }
        ]
    }))
    .unwrap()
}

fn build_request(chart: &ChartSpec) -> QueryRequest {
    let tz_config = TzConfig {
        local_tz: "UTC".to_string(),
        default_input_tz: None,
    };
    let task_scope = chart.to_task_scope().unwrap();
    let tasks = chart.to_tasks(&tz_config, &Default::default()).unwrap();
    let graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let mapping = graph.build_mapping();
    let extent_node = mapping
        .get(&(Variable::new_signal("my_extent"), Vec::new()))
        .cloned()
        .unwrap();

    QueryRequest {
        request: Some(query_request::Request::TaskGraphValues(
            TaskGraphValueRequest {
                task_graph: Some(graph),
                indices: vec![extent_node],
                inline_datasets: vec![],
            },
        )),
    }
}

async fn spawn_server(extra_args: &[String]) -> (ServerProcess, String) {
    let port = pick_unused_port();
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("vegafusion-server"));
    cmd.arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .args(extra_args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = cmd.spawn().expect("Failed to spawn vegafusion-server");
    let address = format!("http://127.0.0.1:{port}");

    for _ in 0..60 {
        if VegaFusionRuntimeClient::connect(address.clone()).await.is_ok() {
            return (ServerProcess { child }, address);
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("Timed out waiting for vegafusion-server to start on port {port}");
}

async fn query_extent(address: String, chart: &ChartSpec) -> std::result::Result<[f64; 2], String> {
    let mut client = VegaFusionRuntimeClient::connect(address)
        .await
        .map_err(|err| err.to_string())?;
    let response = client
        .task_graph_query(build_request(chart))
        .await
        .map_err(|err| err.to_string())?;

    let query_result = response.into_inner();
    match query_result.response.unwrap() {
        Response::Error(error) => Err(format!("{error:?}")),
        Response::TaskGraphValues(values_response) => {
            let response_values = values_response.deserialize().unwrap();
            let (_var, scope, value) = &response_values[0];
            assert_eq!(scope, &Vec::<u32>::new());
            value
                .as_scalar()
                .map_err(|err| err.to_string())?
                .to_f64x2()
                .map_err(|err| err.to_string())
        }
    }
}

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
    let mapping = graph.build_mapping();
    let request = QueryRequest {
        request: Some(query_request::Request::TaskGraphValues(
            TaskGraphValueRequest {
                task_graph: Some(graph),
                indices: vec![
                    mapping
                        .get(&(Variable::new_signal("my_extent"), Vec::new()))
                        .cloned()
                        .unwrap(),
                ],
                inline_datasets: vec![],
            },
        )),
    };

    let (_server, address) = spawn_server(&[]).await;
    let mut client = VegaFusionRuntimeClient::connect(address)
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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_base_url_flag_resolves_relative_urls() {
    let tempdir = tempfile::tempdir().unwrap();
    write_json_rows(tempdir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let args = vec![
        "--base-url".to_string(),
        tempdir.path().to_str().unwrap().to_string(),
        "--allowed-base-url".to_string(),
        tempdir.path().to_str().unwrap().to_string(),
    ];
    let (_server, address) = spawn_server(&args).await;

    let extent = query_extent(address, &extent_spec(json!("data.json")))
        .await
        .unwrap();
    assert_eq!(extent, [1.0, 3.0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_no_base_url_rejects_relative_urls() {
    let tempdir = tempfile::tempdir().unwrap();
    write_json_rows(tempdir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let args = vec!["--no-base-url".to_string()];
    let (_server, address) = spawn_server(&args).await;

    let err = query_extent(address, &extent_spec(json!("data.json")))
        .await
        .unwrap_err();
    assert!(
        err.contains("Relative URL with no base_url configured"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_no_allowed_urls_blocks_external_access() {
    let tempdir = tempfile::tempdir().unwrap();
    let data_path = write_json_rows(tempdir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let args = vec!["--no-allowed-urls".to_string()];
    let (_server, address) = spawn_server(&args).await;

    let err = query_extent(
        address,
        &extent_spec(json!(data_path.to_str().unwrap().to_string())),
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("blocked by allowed_base_urls"),
        "unexpected error: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_server_repeatable_allowed_base_url_flags_allow_multiple_roots() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let blocked_dir = tempfile::tempdir().unwrap();
    write_json_rows(first_dir.path(), "first.json", &[1.0, 2.0, 3.0]);
    let second_path = write_json_rows(second_dir.path(), "second.json", &[4.0, 5.0, 6.0]);
    let blocked_path = write_json_rows(blocked_dir.path(), "blocked.json", &[7.0, 8.0, 9.0]);

    let args = vec![
        "--allowed-base-url".to_string(),
        first_dir.path().to_str().unwrap().to_string(),
        "--allowed-base-url".to_string(),
        second_dir.path().to_str().unwrap().to_string(),
    ];
    let (_server, address) = spawn_server(&args).await;

    let extent = query_extent(
        address.clone(),
        &extent_spec(json!(second_path.to_str().unwrap().to_string())),
    )
    .await
    .unwrap();
    assert_eq!(extent, [4.0, 6.0]);

    let err = query_extent(
        address,
        &extent_spec(json!(blocked_path.to_str().unwrap().to_string())),
    )
    .await
    .unwrap_err();
    assert!(
        err.contains("blocked by allowed_base_urls"),
        "unexpected error: {err}"
    );
}
