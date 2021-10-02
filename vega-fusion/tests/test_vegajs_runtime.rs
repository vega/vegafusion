mod util;
use crate::util::vegajs_runtime::VegaJsRuntime;
use serde_json::json;
use std::collections::HashMap;
use vega_fusion::expression::ast::base::Expression;

#[test]
fn test_vegajs_parse() {
    let vegajs_runtime = VegaJsRuntime::new();
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
    })).unwrap();

    println!("value: {}", parsed);
    assert_eq!(parsed, expected);
}

#[test]
fn test_vegajs_evaluate_scalar() {
    let vegajs_runtime = VegaJsRuntime::new();
    let result = vegajs_runtime.eval_scalar_expression("20 + 300", &Default::default());
    println!("result: {}", result);
    assert_eq!(result, json!(320));
}

#[test]
fn test_vegajs_evaluate_scalar_scope() {
    let vegajs_runtime = VegaJsRuntime::new();
    let scope: HashMap<_, _> = vec![("$a".to_string(), json!(123))].into_iter().collect();
    let result = vegajs_runtime.eval_scalar_expression("20 + a", &scope);
    println!("result: {}", result);
    assert_eq!(result, json!(143));
}
