mod utils;

use prost::Message;

use vegafusion_core::arrow::array::Float64Array;

// use vegafusion_core::expression::parser::parse;
use vegafusion_core::proto::gen::expression;
use vegafusion_core::data::scalar::ScalarValue;
use wasm_bindgen::prelude::*;
use vegafusion_core::proto::gen::tasks::{TaskValue as ProtoTaskValue, Variable, Task, TaskGraph, DataSourceTask, DataUrlTask};
use vegafusion_core::task_graph::task_value::TaskValue;
use std::convert::TryFrom;
use vegafusion_core::task_graph::scope::TaskScope;
use vegafusion_core::proto::gen::transforms::{TransformPipeline, Transform, Extent};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use js_sys::JSON::stringify;
use js_sys::JsString;
use serde_json::{json, Value};

use wasm_bindgen::prelude::*;
use vegafusion_core::spec::chart::ChartSpec;
use web_sys::{HtmlElement, Element};

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

// pub fn make_task_graph() -> TaskGraph {
//     let mut task_scope = TaskScope::new();
//     task_scope.add_variable(&Variable::new_signal("url"), Default::default());
//     task_scope.add_variable(&Variable::new_data("url_datasetA"), Default::default());
//     task_scope.add_variable(&Variable::new_data("datasetA"), Default::default());
//     task_scope.add_data_signal("datasetA", "my_extent", Default::default());
//
//     let tasks = vec![
//         Task::new_value(
//             Variable::new_signal("url"),
//             Default::default(),
//             TaskValue::Scalar(ScalarValue::from("file:///somewhere/over/the/rainbow.csv")),
//         ),
//         Task::new_data_url(Variable::new_data("url_datasetA"), Default::default(), DataUrlTask {
//             url: Some(Url::Expr(parse("url").unwrap())),
//             batch_size: 1024,
//             format_type: None,
//             pipeline: None
//         }),
//         Task::new_data_source(Variable::new_signal("datasetA"), Default::default(), DataSourceTask {
//             source: "url_datasetA".to_string(),
//             pipeline: Some(TransformPipeline { transforms: vec![
//                 Transform { transform_kind: Some(TransformKind::Extent(Extent {
//                     field: "col_1".to_string(),
//                     signal: Some("my_extent".to_string()),
//                 })) }
//             ] })
//         })
//     ];
//
//     TaskGraph::new(tasks, &task_scope).unwrap()
// }

#[wasm_bindgen]
pub fn greet() {

    // let scalar = ScalarValue::from("hello ScalarValue");
    // let task_value = TaskValue::Scalar(scalar.clone());
    // let proto_task_value = ProtoTaskValue::try_from(&task_value).unwrap();
    // let new_task_value = TaskValue::try_from(&proto_task_value).unwrap();
    //
    // let task_graph = make_task_graph();
    // let task_graph_repr = &format!("{:?}", task_graph)[..30];
    //
    // alert(&format!(
    //     "Hello, from Rust!\n{:?}",
    //     task_graph_repr
    // ));

    let spec: ChartSpec = serde_json::from_str(r#"
    {"signals": [{"name": "foo", "value": 23}]}
    "#).unwrap();
    let dataflow = parse(JsValue::from_serde(&spec).unwrap());
    let view = View::new(dataflow);
    let foo = view.get_signal("foo");
    let s1 = stringify(&foo).unwrap();
    view.set_signal("foo", JsValue::from_serde(&json!{ 101 }).unwrap());
    let foo = view.get_signal("foo");
    let s2 = stringify(&foo).unwrap();

    alert(&format!(
        "Hello, {:?} to {:?}",
        s1, s2
    ));
}

#[wasm_bindgen]
pub fn render_vega(element_id: &str) {
    let window: web_sys::Window = web_sys::window().expect("no global `window` exists");
    let document: web_sys::Document = window.document().expect("should have a document on window");
    let mount_element = document.get_element_by_id(element_id).unwrap();

    let chart = spec1();
    let dataflow = parse(JsValue::from_serde(&chart).unwrap());
    let view = View::new(dataflow);
    view.initialize(mount_element);
    view.hover();
    view.run();
}

#[wasm_bindgen(module = "/js/vega_utils.js")]
extern "C" {
    fn vega_version() -> String;
}

#[wasm_bindgen(module = "vega")]
extern "C" {
    pub fn parse(spec: JsValue) -> JsValue;

    type View;

    #[wasm_bindgen(constructor)]
    pub fn new(dataflow: JsValue) -> View;

    #[wasm_bindgen(method, js_name="signal")]
    pub fn get_signal(this: &View, signal: &str) -> JsValue;

    #[wasm_bindgen(method, js_name="signal")]
    pub fn set_signal(this: &View, signal: &str, value: JsValue);

    #[wasm_bindgen(method, js_name="initialize")]
    pub fn initialize(this: &View, container: Element);

    #[wasm_bindgen(method, js_name="run")]
    pub fn run(this: &View);

    #[wasm_bindgen(method, js_name="hover")]
    pub fn hover(this: &View);
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub fn spec1() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "description": "A basic bar chart example, with value labels shown upon mouse hover.",
  "width": 400,
  "height": 200,
  "padding": 5,

  "data": [
    {
      "name": "table",
      "values": [
        {"category": "A", "amount": 28},
        {"category": "B", "amount": 55},
        {"category": "C", "amount": 43},
        {"category": "D", "amount": 91},
        {"category": "E", "amount": 81},
        {"category": "F", "amount": 53},
        {"category": "G", "amount": 19},
        {"category": "H", "amount": 87}
      ]
    }
  ],

  "signals": [
    {
      "name": "tooltip",
      "value": {},
      "on": [
        {"events": "rect:mouseover", "update": "datum"},
        {"events": "rect:mouseout",  "update": "{}"}
      ]
    }
  ],

  "scales": [
    {
      "name": "xscale",
      "type": "band",
      "domain": {"data": "table", "field": "category"},
      "range": "width",
      "padding": 0.05,
      "round": true
    },
    {
      "name": "yscale",
      "domain": {"data": "table", "field": "amount"},
      "nice": true,
      "range": "height"
    }
  ],

  "axes": [
    { "orient": "bottom", "scale": "xscale" },
    { "orient": "left", "scale": "yscale" }
  ],

  "marks": [
    {
      "type": "rect",
      "from": {"data":"table"},
      "encode": {
        "enter": {
          "x": {"scale": "xscale", "field": "category"},
          "width": {"scale": "xscale", "band": 1},
          "y": {"scale": "yscale", "field": "amount"},
          "y2": {"scale": "yscale", "value": 0}
        },
        "update": {
          "fill": {"value": "steelblue"}
        },
        "hover": {
          "fill": {"value": "red"}
        }
      }
    },
    {
      "type": "text",
      "encode": {
        "enter": {
          "align": {"value": "center"},
          "baseline": {"value": "bottom"},
          "fill": {"value": "#333"}
        },
        "update": {
          "x": {"scale": "xscale", "signal": "tooltip.category", "band": 0.5},
          "y": {"scale": "yscale", "signal": "tooltip.amount", "offset": -2},
          "text": {"signal": "tooltip.amount"},
          "fillOpacity": [
            {"test": "datum === tooltip", "value": 0},
            {"value": 1}
          ]
        }
      }
    }
  ]
}
    "##).unwrap()
}