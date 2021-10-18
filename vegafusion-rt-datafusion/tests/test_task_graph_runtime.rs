use vegafusion_core::task_graph::scope::TaskScope;
use vegafusion_core::proto::gen::tasks::{Variable, Task, ScanUrlTask, TransformsTask, TaskGraph};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::proto::gen::transforms::{TransformPipeline, Transform, Extent, Collect, SortOrder};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
use std::sync::Arc;


#[tokio::test(flavor="multi_thread")]
async fn try_it() {
    let mut task_scope = TaskScope::new();
    task_scope.add_variable(&Variable::new_signal("url"), Default::default());
    task_scope.add_variable(&Variable::new_data("url_datasetA"), Default::default());
    task_scope.add_variable(&Variable::new_data("datasetA"), Default::default());
    task_scope.add_data_signal("datasetA", "my_extent", Default::default());

    let tasks = vec![
        Task::new_value(
            Variable::new_signal("url"),
            Default::default(),
            TaskValue::Scalar(ScalarValue::from("https://raw.githubusercontent.com/vega/vega-datasets/master/data/penguins.json")),
        ),
        Task::new_scan_url(Variable::new_data("url_datasetA"), Default::default(), ScanUrlTask {
            url: Some(Variable::new_signal("url")),
            batch_size: 1024,
            format_type: None
        }),
        Task::new_transforms(Variable::new_data("datasetA"), Default::default(), TransformsTask {
            source: "url_datasetA".to_string(),
            pipeline: Some(TransformPipeline { transforms: vec![
                Transform { transform_kind: Some(TransformKind::Extent(Extent {
                    field: "Beak Length (mm)".to_string(),
                    signal: Some("my_extent".to_string()),
                })) },
                Transform { transform_kind: Some(TransformKind::Collect(Collect {
                    fields: vec!["Beak Length (mm)".to_string()],
                    order: vec![SortOrder::Ascending as i32]
                })) },
            ] })
        })
    ];

    let graph = Arc::new(TaskGraph::new(tasks, task_scope).unwrap());

    let graph_runtime = TaskGraphRuntime::new(20);
    // let result = graph_runtime.get_node_value(graph, 2, None).await.unwrap();
    let result = graph_runtime.get_node_value(graph, 2, Some(0)).await.unwrap();

    println!("result: {:?}", result);

    // println!("graph:\n{:#?}", graph);
}
