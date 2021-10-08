#[macro_use]
extern crate lazy_static;

mod util;
use crate::util::vegajs_runtime::vegajs_runtime;
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use serde_json::json;
use std::collections::HashMap;
use vega_fusion::data::table::VegaFusionTable;
use vega_fusion::expression::ast::base::Expression;
use vega_fusion::expression::compiler::config::CompilationConfig;
use vega_fusion::spec::transform::extent::ExtentTransformSpec;
use vega_fusion::spec::transform::filter::FilterTransformSpec;
use vega_fusion::spec::transform::TransformSpec;

#[test]
fn test_vegajs_parse() {
    let vegajs_runtime = vegajs_runtime();
    let parsed = vegajs_runtime.parse_expression("(20 + 5) * 300").unwrap();

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
    let vegajs_runtime = vegajs_runtime();
    let result = vegajs_runtime
        .eval_scalar_expression("20 + 300", &Default::default())
        .unwrap();
    println!("result: {}", result);
    assert_eq!(result, ScalarValue::from(320.0));
}

#[test]
fn test_vegajs_evaluate_scalar_scope() {
    let vegajs_runtime = vegajs_runtime();
    let scope: HashMap<_, _> = vec![("a".to_string(), ScalarValue::from(123.0))]
        .into_iter()
        .collect();
    let result = vegajs_runtime
        .eval_scalar_expression("20 + a", &scope)
        .unwrap();
    println!("result: {}", result);
    assert_eq!(result, ScalarValue::from(143.0));
}

#[test]
fn test_evaluate_filter_transform() {
    let vegajs_runtime = vegajs_runtime();
    let dataset = VegaFusionTable::from_json(
        json!([
            {"colA": 2.0, "colB": false, "colC": "first"},
            {"colA": 4.0, "colB": true, "colC": "second"},
            {"colA": 6.0, "colB": false, "colC": "third"},
            {"colA": 8.0, "colB": true, "colC": "forth"},
            {"colA": 10.0, "colB": false, "colC": "fifth"},
        ]),
        1024,
    )
    .unwrap();

    let signal_scope: HashMap<_, _> = vec![("a".to_string(), ScalarValue::from(6.0))]
        .into_iter()
        .collect();
    let config = CompilationConfig {
        signal_scope,
        ..Default::default()
    };

    let transforms = vec![
        TransformSpec::Filter(FilterTransformSpec {
            expr: "datum.colA >= a".to_string(),
            extra: Default::default(),
        }),
        TransformSpec::Extent(ExtentTransformSpec {
            field: "colA".to_string(),
            signal: Some("extent_out".to_string()),
            extra: Default::default(),
        }),
    ];

    let (result_data, result_signals) = vegajs_runtime
        .eval_transform(&dataset, &transforms, &config)
        .unwrap();

    println!("{}\n", result_data.pretty_format(None).unwrap());
    println!("{:#?}\n", result_signals);

    // Check extent signal
    assert_eq!(
        result_signals,
        vec![(
            "extent_out".to_string(),
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::from(6.0),
                    ScalarValue::from(10.0)
                ])),
                Box::new(DataType::Float64)
            )
        )]
        .into_iter()
        .collect()
    );

    let expected_dataset = VegaFusionTable::from_json(
        json!([
            {"colA": 6, "colB": false, "colC": "third"},
            {"colA": 8, "colB": true, "colC": "forth"},
            {"colA": 10, "colB": false, "colC": "fifth"},
        ]),
        1024,
    )
    .unwrap();

    assert_eq!(result_data.to_json(), expected_dataset.to_json());
}
