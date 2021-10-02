#[macro_use]
extern crate lazy_static;

mod util;
use crate::util::vegajs_runtime::vegajs_runtime;
use serde_json::{json, Value};
use std::collections::HashMap;
use vega_fusion::expression::ast::base::Expression;

#[test]
fn test_vegajs_parse() {
    let mut vegajs_runtime = vegajs_runtime();
    let parsed = vegajs_runtime.parse_expression("(20 + 5) * 300");

    let expected: Expression = serde_json::value::from_value(json!({
        "type":"BinaryExpression",
        "left":{
            "type":"BinaryExpression",
            "left":{"type":"Literal","value":20.0,"raw":"20"},
            "operator":"+",
            "right":{"type":"Literal","value":5.0,"raw":"5"}},
        "operator":"*",
        "right":{"type":"Literal","value":300.0,"raw":"300"}
    }))
    .unwrap();

    println!("value: {}", parsed);
    assert_eq!(parsed, expected);
}

#[test]
fn test_vegajs_evaluate_scalar() {
    let mut vegajs_runtime = vegajs_runtime();
    let result = vegajs_runtime.eval_scalar_expression("20 + 300", &Default::default());
    println!("result: {}", result);
    assert_eq!(result, json!(320));
}

#[test]
fn test_vegajs_evaluate_scalar_scope() {
    let mut vegajs_runtime = vegajs_runtime();
    let scope: HashMap<_, _> = vec![("$a".to_string(), json!(123))].into_iter().collect();
    let result = vegajs_runtime.eval_scalar_expression("20 + a", &scope);
    println!("result: {}", result);
    assert_eq!(result, json!(143));
}

fn bar_chart_spec() -> Value {
    json!({
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
        { "orient": "bottom", "scale": "xscale"},
        { "orient": "left", "scale": "yscale"}
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
    })
}

#[test]
fn try_vegajs_convert_to_svg() {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let mut vegajs_runtime = vegajs_runtime();
    let spec = bar_chart_spec();
    let result = vegajs_runtime.convert_to_svg(&spec);
    std::fs::write(format!("{}/tests/scratch/test.svg", crate_dir), &result).unwrap();
}

#[test]
fn try_vegajs_convert_to_png() {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let mut vegajs_runtime = vegajs_runtime();
    let spec = bar_chart_spec();
    vegajs_runtime.save_to_png(
        &spec,
        std::path::Path::new(&format!("{}/tests/scratch/test2.png", crate_dir)),
    );
}
