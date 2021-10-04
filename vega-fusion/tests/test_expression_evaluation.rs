#[macro_use]
extern crate lazy_static;

mod util;
use datafusion::scalar::ScalarValue;
use rstest::rstest;
use std::collections::HashMap;
use util::vegajs_runtime::vegajs_runtime;
use vega_fusion::expression::compiler::compile;
use vega_fusion::expression::compiler::config::CompilationConfig;
use vega_fusion::expression::compiler::utils::ExprHelpers;
use vega_fusion::expression::parser::parse;

fn scope_a() -> HashMap<String, ScalarValue> {
    vec![
        ("foo", ScalarValue::from(23.5)),
        ("bar", ScalarValue::from(100.0)),
        ("valid", ScalarValue::from(false)),
        ("hello", ScalarValue::from("Hello")),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

fn check_evaluation(expr_str: &str, scope: &HashMap<String, ScalarValue>) {
    // Use block here to drop vegajs_runtime lock before the potential assert_eq error
    // This avoids poisoning the Mutex if the assertion fails
    let expected = {
        let mut vegajs_runtime = vegajs_runtime();
        vegajs_runtime.eval_scalar_expression(expr_str, scope)
    };

    // Vega-Fusion parse
    let parsed = parse(expr_str).unwrap();

    // Build compilation config
    let config = CompilationConfig {
        signal_scope: scope.clone(),
        ..Default::default()
    };

    let compiled = compile(&parsed, &config, None).unwrap();
    let result = compiled.eval_to_scalar().unwrap();

    assert_eq!(
        result,
        expected,
        " left: {}\nright: {}\n",
        result.to_string(),
        expected.to_string()
    );
}

mod test_atoms {
    use crate::*;

    #[rstest(
        expr,
        case("true"),
        case("false"),
        case("25"),
        case("25.5"),
        case("'hello'"),
        case("\"world\"")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_binary_kinds {
    use crate::*;

    #[rstest(
        expr,
        case("2 + 1"),
        case("2 * 1"),
        case("2 / 1"),
        case("2 % 1"),
        case("'hello' + \"world\""),
        case("2 * bar"),
        case("2 / foo")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_binary_precedence {
    use crate::*;

    #[rstest(
        expr,
        case("1 + 2 * 3 / 4 % 6 / 7 * 8"),
        case("(1 + 2) * 3 / 4 % 6 / 7 * 8"),
        case("1 + (2 * 3) / 4 % 6 / 7 * 8"),
        case("1 + 2 * (3 / 4) % 6 / 7 * 8"),
        case("1 + 2 * 3 / (4 % 6) / 7 * 8"),
        case("1 + 2 * 3 / 4 % (6 / 7) * 8"),
        case("1 + 2 * 3 / 4 % 6 / (7 * 8)"),
        case("1 + 2 * 3 / 4 % 6 / 7 * (8 + 9)")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_unary {
    use crate::*;

    #[rstest(
        expr,
        case("-1.5"),
        case("!false"),
        case("!foo"),
        case("!!!!valid"),
        case("+'34'"),
        case("-(2 % 1)"),
        case("2 + -3"),
        case("-+-+-3"),
        case("(-(-(-3)))")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_logical {
    use crate::*;

    #[rstest(
        expr,
        case("false && true"),
        case("valid && true"),
        case("1 && 2 || 3 && 4 || 5"),
        case("(1 && 2) || 3 && 4 || 5"),
        case("1 && (2 || 3) && 4 || 5"),
        case("1 && 2 || (3 && 4) || 5"),
        case("1 && 2 || 3 && (4 || 5)"),
        case("1 && valid || !valid && (4 || 5)")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_ternary {
    use crate::*;

    #[rstest(
        expr,
        case("true? -10: 10"),
        case("1 + -1? 8: 23"),
        case("1 + (-1? 8: 23)"),
        case("1? 2: 3? 4: 5? 6: 7"),
        case("(1? 2: (3? 4: (5? 6: 7)))"),
        case("(((1? 2: 3)? 4: 5)? 6: 7)")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_call {
    use crate::*;

    #[rstest(
        expr,
        case("sqrt(16)"),
        case("round(1.2) + round(1.8)"),
        case("isNaN(16) + isNaN(NaN)")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_member_access {
    use crate::*;

    #[rstest(
        expr,
        case("({foo: 10, bar: 23})['bar']"),
        case("({foo: 10, bar: 23}).foo"),
        case("[1, 2, 3][1]"),
        case("({foo: {bar: 10}}).foo"),
        case("({foo: {bar: 10}}).foo.bar"),
        case("({foo: {bar: 10}})['foo']['bar']")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_array_expression {
    use crate::*;

    #[rstest(
        expr,
        case("[1]"),
        case("[1, 2]"),
        case("['a', 'b']"),
        case("[['a', 'b'], ['c', 'd']]"),
        case("[]")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_object_expression {
    use crate::*;

    #[rstest(
    expr,
    // case("{}"),  // todo: How should we support empty objects?
    case("{a: 10}"),
    case("{a: hello}"),
    case("{'a': 10, b: 23,}"),
    case("{11: 'b', 22: 'a'}"),
    case("{17: 9, a: 10, 'b': 11}")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_datetime {
    use crate::*;

    #[rstest(
        expr,
        case("datetime('2020-05-16T09:30:00+05:00')"),
        case("datetime('2020-05-16 09:30:00+05:00')"),
        case("datetime('2020-05-16 09:30:00-07:00')"),
        case("datetime('2020-05-16 09:30:00Z')"),
        case("datetime('2020-05-16 09:30:00')"),
        case("datetime(1589603400000)"),
        case("datetime(87, 3, 10, 7, 35, 10, 87)"),
        case("datetime(87, 3, 10, 7, 35, 10)"),
        case("datetime(87, 3, 10, 7, 35)"),
        case("datetime(87, 3, 10, 7)"),
        case("datetime(87, 3, 10)"),
        case("datetime(87, 3)"),
        case("datetime(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("datetime(utc(87, 3, 10, 7, 35, 10))"),
        case("datetime(utc(87, 3, 10, 7, 35))"),
        case("datetime(utc(87, 3, 10, 7))"),
        case("datetime(utc(87, 3, 10))"),
        case("datetime(utc(87, 3))")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_date_parts {
    use crate::*;

    #[rstest(
        expr,
        case("year(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcyear(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("month(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcmonth(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("date(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcdate(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("hours(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utchours(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("minutes(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcminutes(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("seconds(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcseconds(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("milliseconds(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0"),
        case("utcmilliseconds(datetime(utc(87, 3, 10, 7, 35, 10, 87))) + 0")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_length {
    use crate::*;

    #[rstest(
        expr,
        // Below: Add 0 to force result type to f64 even though length returns i32
        // case("length(datum.colC) + 0"),
        case("length([1, 2, 3]) + 0"),
        case("[1, 2, 3].length + 0"),
        case("length('abc') + 0"),
        case("'abc'.length + 0"),
        case("hello.length + 0"),
        case("length(hello) + 0"),
        // case("length(data('dataB')) + 0"),
        // case("data('dataB').length + 0"),
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}

mod test_get_index {
    use crate::*;

    #[rstest(
        expr,
        case("[1, 2, 3][1]"),
        case("'Hello!'[3]"),
        // case("datum.colC[2]"),
        // case("data('dataB')[1].colA")
    )]
    fn test(expr: &str) {
        check_evaluation(expr, &scope_a())
    }
}
