/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
// use std::sync::Arc;
// use vegafusion_core::data::scalar::ScalarValue;
// use vegafusion_core::expression::parser::parse;
// use vegafusion_core::proto::gen::tasks::data_url_task::Url;
// use vegafusion_core::proto::gen::tasks::{
//     DataSourceTask, DataUrlTask, NodeValueIndex, Task, TaskGraph, Variable,
// };
// use vegafusion_core::proto::gen::transforms::transform::TransformKind;
// use vegafusion_core::proto::gen::transforms::{
//     Collect, Extent, SortOrder, Transform, TransformPipeline,
// };
// use vegafusion_core::spec::chart::ChartSpec;
// use vegafusion_core::task_graph::scope::TaskScope;
// use vegafusion_core::task_graph::task_value::TaskValue;
// use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
//

use assert_cmd::prelude::*;
use std::time::Duration;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::proto::gen::services::query_result::Response;
use vegafusion_core::proto::gen::services::vega_fusion_runtime_client::VegaFusionRuntimeClient;
use vegafusion_core::proto::gen::services::{query_request, QueryRequest};
use vegafusion_core::proto::gen::tasks::{
    NodeValueIndex, TaskGraph, TaskGraphValueRequest, VariableNamespace,
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

    let task_scope = chart.to_task_scope().unwrap();
    let tasks = chart.to_tasks().unwrap();

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
    let cmd = bin.args(&["--port", "50059"]);

    let mut proc = cmd.spawn().expect("Failed to spawn vegafusion-server");
    std::thread::sleep(Duration::from_millis(2000));

    let mut client = VegaFusionRuntimeClient::connect("http://127.0.0.1:50059")
        .await
        .expect("Failed to connect to gRPC server");
    let response = client.task_graph_query(request).await.unwrap();

    let query_result = response.into_inner();
    match query_result.response.unwrap() {
        Response::Error(error) => {
            panic!("Error: {:?}", error)
        }
        Response::TaskGraphValues(values_response) => {
            let response_values = values_response.deserialize().unwrap();
            println!("Result: {:#?}", response_values);
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
