mod util;
use crate::util::vegajs_runtime::VegaJsRuntime;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn try_vegajs_parse() {
    let vegajs_runtime = VegaJsRuntime::new();
    let parsed = vegajs_runtime.parse_expression("(20 + 5) * 300");
    println!("value: {:?}", parsed);
    println!("value: {}", parsed);
}

#[test]
fn try_vegajs_evaluate_scalar() {
    let vegajs_runtime = VegaJsRuntime::new();
    let result = vegajs_runtime.eval_scalar_expression("20 + 300", &Default::default());
    println!("result: {}", result);
}

#[test]
fn try_vegajs_evaluate_scalar_scope() {
    let vegajs_runtime = VegaJsRuntime::new();
    let scope: HashMap<_, _> = vec![("$a".to_string(), json!(123))].into_iter().collect();
    let result = vegajs_runtime.eval_scalar_expression("20 + a", &scope);
    println!("result: {}", result);
}
