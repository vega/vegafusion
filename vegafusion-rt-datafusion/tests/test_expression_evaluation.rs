/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
#[macro_use]
extern crate lazy_static;

mod util;
use datafusion::scalar::ScalarValue;
use rstest::rstest;
use serde_json::json;
use std::collections::HashMap;

use util::check::check_scalar_evaluation;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;

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

pub fn dataset_1() -> VegaFusionTable {
    let json_value = json!([
        {"colA": 2.0, "colB": false, "colC": "first"},
        {"colA": 4.0, "colB": true, "colC": "second"},
        {"colA": 6.0, "colB": false, "colC": "third"},
    ]);
    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}

pub fn dataset_2() -> VegaFusionTable {
    let json_value = json!([
        {"colA": 40.0, "colB": true, "colC": "SECOND"},
        {"colA": 60.0, "colB": false, "colC": "THIRD"},
    ]);
    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}

fn datasets() -> HashMap<String, VegaFusionTable> {
    vec![
        ("dataA".to_string(), dataset_1()),
        ("dataB".to_string(), dataset_2()),
    ]
    .into_iter()
    .collect()
}

fn config_a() -> CompilationConfig {
    CompilationConfig {
        signal_scope: scope_a(),
        data_scope: datasets(),
        ..Default::default()
    }
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        case("datetime('2020/05/16 09:30')"),
        case("datetime('05/16/2020 09:30')"),
        case("datetime('May 16 2020 09:30')"),
        case("datetime('July 15, 2010')"),
        case("datetime('2020 May 16  09:30')"),
        case("datetime('2020-01-01 00:00')"),
        case("datetime('2020-01-01')"),
        case("datetime('2020/01/01')"),
        case("datetime('01/01/2020')"),
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
        case("datetime(utc(87, 3))"),
        case("datetime(\"2000-01-01T08:00:00.000Z\")"),
        case("datetime(\"2000-01-01T13:14:15.123Z\")")
    )]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_time {
    use crate::*;

    #[rstest(
        expr,
        case("time('2020-05-16T09:30:00+05:00')"),
        case("time('2020-05-16 09:30:00+05:00')"),
        case("time('2020-05-16 09:30:00-07:00')"),
        case("time('2020-05-16 09:30:00Z')"),
        case("time('2020-05-16 09:30:00')"),
        case("time('2020/05/16 09:30')"),
        case("time('05/16/2020 09:30')"),
        case("time('May 16 2020 09:30')"),
        case("time('2020 May 16  09:30')"),
        case("time('2020-01-01 00:00')"),
        case("time('2020-01-01')"),
        case("time('2020/01/01')"),
        case("time('01/01/2020')"),
        case("time(1589603400000)"),
        case("time(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("time(datetime(87, 3, 10, 7, 35, 10))"),
        case("time(datetime(87, 3, 10, 7, 35))"),
        case("time(datetime(87, 3, 10, 7))"),
        case("time(datetime(87, 3, 10))"),
        case("time(datetime(87, 3))"),
        case("time(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("time(datetime(utc(87, 3, 10, 7, 35, 10)))"),
        case("time(datetime(utc(87, 3, 10, 7, 35)))"),
        case("time(datetime(utc(87, 3, 10, 7)))"),
        case("time(datetime(utc(87, 3, 10)))"),
        case("time(datetime(utc(87, 3)))"),
        case("time(\"2000-01-01T08:00:00.000Z\")"),
        case("time(\"2000-01-01T13:14:15.123Z\")")
    )]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_time_and_utc_format {
    use crate::*;

    #[rstest(
        expr,
        case("timeFormat(toDate('2020-05-16T09:30:00+05:00'), '%Y-%m-%d %H:%M:%S')"),
        case("utcFormat(toDate('2020-05-16T09:30:00+05:00'), '%Y-%m-%d %H:%M:%S')"),
        case("timeFormat(toDate('2020-05-16 09:30:00+05:00'))"),
        case("utcFormat(toDate('2020-05-16 09:30:00+05:00'))"),
        case("timeFormat(1589603400000, '%Y-%m-%d %p %s')"),
        case("utcFormat(1589603400000, '%Y-%m-%d %G %g')"),
        case("timeFormat(datetime(87, 3, 10, 7, 35, 10, 87), '%a %A %b %B %d %e %g')"),
        case("utcFormat(datetime(87, 3, 10, 7, 35, 10, 87), '%a %A %b %B %d %e %g')"),
        case("timeFormat(datetime(87, 3, 10, 7, 35, 10, 87), '%Y-%m-%d %H:%M:%S.%L')"),
        case("utcFormat(datetime(87, 3, 10, 7, 35, 10, 87), '%Y-%m-%d %H:%M:%S.%f')")
    )]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_date_parts {
    #[rstest(
        expr,
        case("year(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("year(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("year(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcyear(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcyear(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcyear(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("quarter(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("quarter(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("quarter(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcquarter(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcquarter(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcquarter(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("month(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("month(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("month(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcmonth(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcmonth(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcmonth(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("day(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("day(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("day(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcday(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcday(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcday(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("date(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("date(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("date(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcdate(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcdate(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcdate(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("dayofyear(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("dayofyear(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("dayofyear(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcdayofyear(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcdayofyear(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcdayofyear(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("hours(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("hours(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("hours(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utchours(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utchours(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utchours(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("minutes(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("minutes(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("minutes(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcminutes(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcminutes(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcminutes(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("seconds(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("seconds(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("seconds(datetime(87, 3, 10, 7, 35, 10, 87))"),
        case("utcseconds(datetime(utc(87, 3, 10, 7, 35, 10, 87)))"),
        case("utcseconds(utc(87, 3, 10, 7, 35, 10, 87))"),
        case("utcseconds(datetime(87, 3, 10, 7, 35, 10, 87))")
    )]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    use crate::*;

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_length {
    use crate::*;

    #[rstest(
        expr,
        // Below: Add 0 to force result type to f64 even though length returns i32
        case("length([1, 2, 3])"),
        case("[1, 2, 3].length"),
        case("length('abc')"),
        case("'abc'.length"),
        case("hello.length"),
        case("length(hello)"),
        case("length(data('dataB'))"),
        case("data('dataB').length"),
    )]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
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
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_null_comparison {
    use crate::*;

    #[rstest(expr, case("1 === null"), case("1 !== null"))]
    fn test(expr: &str) {
        check_scalar_evaluation(expr, &config_a())
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
