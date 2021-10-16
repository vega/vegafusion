mod utils;

use prost::Message;

use vegafusion_core::arrow::array::Float64Array;

use vegafusion_core::expression::parser::parse;
use vegafusion_core::proto::gen::expression;
use vegafusion_core::data::scalar::ScalarValue;
use wasm_bindgen::prelude::*;
use vegafusion_core::proto::gen::tasks::{TaskValue as ProtoTaskValue, Variable, Task, ScanUrlTask, TransformsTask, TaskGraph};
use vegafusion_core::task_graph::task_value::TaskValue;
use std::convert::TryFrom;
use vegafusion_core::task_graph::scope::TaskScope;
use vegafusion_core::proto::gen::transforms::{TransformPipeline, Transform, Extent};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;


// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

pub fn make_task_graph() -> TaskGraph {
    let mut task_scope = TaskScope::new();
    task_scope.add_variable(&Variable::new_signal("url"), Default::default());
    task_scope.add_variable(&Variable::new_data("url_datasetA"), Default::default());
    task_scope.add_variable(&Variable::new_data("datasetA"), Default::default());
    task_scope.add_data_signal("datasetA", "my_extent", Default::default());

    let tasks = vec![
        Task::new_value(
            Variable::new_signal("url"),
            Default::default(),
            TaskValue::Scalar(ScalarValue::from("file:///somewhere/over/the/rainbow.csv")),
        ),
        Task::new_scan_url(Variable::new_data("url_datasetA"), Default::default(), ScanUrlTask {
            url: Some(Variable::new_signal("url")),
            batch_size: 1024,
            format_type: None
        }),
        Task::new_transforms(Variable::new_signal("datasetA"), Default::default(), TransformsTask {
            source: "url_datasetA".to_string(),
            pipeline: Some(TransformPipeline { transforms: vec![
                Transform { transform_kind: Some(TransformKind::Extent(Extent {
                    field: "col_1".to_string(),
                    signal: Some("my_extent".to_string()),
                })) }
            ] })
        })
    ];

    TaskGraph::new(tasks, task_scope).unwrap()
}

#[wasm_bindgen]
pub fn greet() {
    // let lit = expression::Literal {
    //     raw: "23.5000".to_string(),
    //     value: Some(expression::literal::Value::Number(23.5)),
    // };
    //
    // let arr = Float64Array::from(vec![1.0, 2.0, 3.0]);
    // let expr = parse("2 + 3").unwrap();
    // let mut expr_bytes = Vec::new();
    // expr_bytes.reserve(expr.encoded_len());
    // // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    // expr.encode(&mut expr_bytes).unwrap();

    let scalar = ScalarValue::from("hello ScalarValue");
    let task_value = TaskValue::Scalar(scalar.clone());
    let proto_task_value = ProtoTaskValue::try_from(&task_value).unwrap();
    let new_task_value = TaskValue::try_from(&proto_task_value).unwrap();

    let task_graph = make_task_graph();
    let task_graph_repr = &format!("{:?}", task_graph)[..30];

    alert(&format!(
        "Hello, from Rust!\n{:?}",
        task_graph_repr
    ));
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
