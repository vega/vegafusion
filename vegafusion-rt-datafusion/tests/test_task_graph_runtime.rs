/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::expression::parser::parse;
use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use vegafusion_core::proto::gen::tasks::{
    DataSourceTask, DataUrlTask, NodeValueIndex, Task, TaskGraph, TzConfig, Variable,
};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::proto::gen::transforms::{
    Collect, Extent, SortOrder, Transform, TransformPipeline,
};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::scope::TaskScope;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

#[tokio::test(flavor = "multi_thread")]
async fn try_it() {
    let tz_config = TzConfig {
        local_tz: "America/New_York".to_string(),
        default_input_tz: None,
    };
    let mut task_scope = TaskScope::new();
    task_scope
        .add_variable(&Variable::new_signal("url"), Default::default())
        .unwrap();
    task_scope
        .add_variable(&Variable::new_data("url_datasetA"), Default::default())
        .unwrap();
    task_scope
        .add_variable(&Variable::new_data("datasetA"), Default::default())
        .unwrap();
    task_scope
        .add_data_signal("datasetA", "my_extent", Default::default())
        .unwrap();

    let tasks = vec![
        Task::new_value(
            Variable::new_signal("url"),
            Default::default(),
            TaskValue::Scalar(ScalarValue::from(
                "https://raw.githubusercontent.com/vega/vega-datasets/master/data/penguins.json",
            )),
        ),
        Task::new_data_url(
            Variable::new_data("url_datasetA"),
            Default::default(),
            DataUrlTask {
                url: Some(Url::Expr(parse("url").unwrap())),
                batch_size: 1024,
                format_type: None,
                pipeline: None,
            },
            &tz_config,
        ),
        Task::new_data_source(
            Variable::new_data("datasetA"),
            Default::default(),
            DataSourceTask {
                source: "url_datasetA".to_string(),
                pipeline: Some(TransformPipeline {
                    transforms: vec![
                        Transform {
                            transform_kind: Some(TransformKind::Extent(Extent {
                                field: "Beak Length (mm)".to_string(),
                                signal: Some("my_extent".to_string()),
                            })),
                        },
                        Transform {
                            transform_kind: Some(TransformKind::Collect(Collect {
                                fields: vec!["Beak Length (mm)".to_string()],
                                order: vec![SortOrder::Ascending as i32],
                            })),
                        },
                    ],
                }),
            },
            &tz_config,
        ),
    ];

    let graph = Arc::new(TaskGraph::new(tasks, &task_scope).unwrap());

    let graph_runtime = TaskGraphRuntime::new(Some(20), Some(1024_i32.pow(3) as usize));
    // let result = graph_runtime.get_node_value(graph, 2, None).await.unwrap();
    let result = graph_runtime
        .get_node_value(graph, &NodeValueIndex::new(2, Some(0)), Default::default())
        .await
        .unwrap();

    println!("result: {:?}", result);
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

    let tz_config = TzConfig {
        local_tz: "America/New_York".to_string(),
        default_input_tz: None,
    };
    let task_scope = chart.to_task_scope().unwrap();
    let tasks = chart.to_tasks(&tz_config, &Default::default()).unwrap();

    println!("task_scope: {:?}", task_scope);
    println!("tasks: {:?}", tasks);

    let graph = Arc::new(TaskGraph::new(tasks, &task_scope).unwrap());

    let graph_runtime = TaskGraphRuntime::new(Some(20), Some(1024_i32.pow(3) as usize));
    let result = graph_runtime
        .get_node_value(graph, &NodeValueIndex::new(2, Some(0)), Default::default())
        .await
        .unwrap();
    println!("result: {:?}", result);
}
