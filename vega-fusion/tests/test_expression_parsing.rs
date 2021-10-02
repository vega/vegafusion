#[macro_use]
extern crate lazy_static;

mod util;
use util::vegajs_runtime::vegajs_runtime;
use vega_fusion::expression::parser::parse;

#[test]
fn test_vegajs_parse() {
    let mut vegajs_runtime = vegajs_runtime();
    let expr_str = "(20 + 5) * 300";
    let expected = vegajs_runtime.parse_expression(expr_str);
    let result = parse(expr_str).unwrap();

    println!("value: {}", result);
    assert_eq!(result.to_string(), expected.to_string());
}
