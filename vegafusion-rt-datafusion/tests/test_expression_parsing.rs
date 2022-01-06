/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
#[macro_use]
extern crate lazy_static;

mod util;
use crate::util::check::check_expr_supported;
use rstest::rstest;

use util::check::check_parsing;

mod test_parse_atoms {
    use crate::*;

    #[rstest(
        expr,
        case("true"),
        case("false"),
        case("25"),
        case("25.5"),
        case("'hello'"),
        case("\"world\""),
        case("ident")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_binary {
    use crate::*;

    #[rstest(
        expr,
        case("2 + 1"),
        case("2 * 1"),
        case("2 / 1"),
        case("2 % 1"),
        case("'hello' + \"world\"")
    )]

    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_binary_precedence {
    use crate::*;

    #[rstest(
        expr,
        case("1 + 2 * 3 / 4 % 6 / 7 * 8 + 9"),
        case("(1 + 2) * 3 / 4 % 6 / 7 * 8 + 9"),
        case("1 + (2 * 3) / 4 % 6 / 7 * 8 + 9"),
        case("1 + 2 * (3 / 4) % 6 / 7 * 8 + 9"),
        case("1 + 2 * 3 / (4 % 6) / 7 * 8 + 9"),
        case("1 + 2 * 3 / 4 % (6 / 7) * 8 + 9"),
        case("1 + 2 * 3 / 4 % 6 / (7 * 8) + 9"),
        case("1 + 2 * 3 / 4 % 6 / 7 * (8 + 9)")
    )]

    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_unary {
    use crate::*;

    #[rstest(
        expr,
        case("-1.5"),
        case("!false"),
        case("+'34'"),
        case("-(2 % 1)"),
        case("2 + -3"),
        case("-+-+-3"),
        case("(-(-(-3)))")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_logical {
    use crate::*;

    #[rstest(
        expr,
        case("2 && true"),
        case("foo || bar"),
        case("1 && 2 || 3 && 4 || 5"),
        case("(1 && 2) || 3 && 4 || 5"),
        case("1 && (2 || 3) && 4 || 5"),
        case("1 && 2 || (3 && 4) || 5"),
        case("1 && 2 || 3 && (4 || 5)")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_ternary {
    use crate::*;

    #[rstest(
        expr,
        case("true? foo: bar"),
        case("1 + 10? foo * 8: bar + 23"),
        case("1 + (10? foo * 8: bar) + 23"),
        case("1? 2: 3? 4: 5? 6: 7"),
        case("(1? 2: (3? 4: (5? 6: 7)))"),
        case("(((1? 2: 3)? 4: 5)? 6: 7)")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_call {
    use crate::*;

    #[rstest(
        expr,
        case("foo()"),
        case("foo(10)"),
        case("foo('a', false, 23)"),
        case("3 + foo('a', false || bar, 23)")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_computed_member_access {
    use crate::*;

    #[rstest(
        expr,
        case("foo[0]"),
        case("foo['bar']"),
        case("foo[\"bar\"]"),
        case("foo[0 + 23]"),
        case("foo[0]['a']"),
        case("foo[25]['a'][0]")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_static_member_access {
    use crate::*;

    #[rstest(
        expr,
        case("foo.bar"),
        case("foo.bar.baz.foo"),
        case("foo.bar.baz[0] + 23")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_array_literal {
    use crate::*;

    #[rstest(
        expr,
        case("[]"),
        case("[1]"),
        case("[1, 2]"),
        case("[1, 2 + 3, ['a', 'b']]")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_parse_object_literal {
    use crate::*;

    #[rstest(
        expr,
        case("{}"),
        case("{a: 10}"),
        case("{'a': 10}"),
        case("{22: 'a'}"),
        case("{a: 10, 'b': 11, 17: 9}")
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

// Expressions extracted from various Vega specs in the gallery
mod test_parse_examples {
    use crate::*;

    #[rstest(
        expr,
        case("isValid(datum[\"average_b\"]) && isFinite(+datum[\"average_b\"])"),
        case("!isValid(datum[\"bin_maxbins_10_precipitation\"]) || !isFinite(+datum[\"bin_maxbins_10_precipitation\"])"),
        case("datum.temp_max - datum.temp_min"),
        case("datum.year == 2000"),
        case("datum.sex == 2 ? 'Female' : 'Male'"),
        case("datum.sex == 2 ? -datum.people : datum.people"),
        case("if(datum.type === 'Strongly disagree',-2,0) + if(datum.type==='Disagree',-1,0) + if(datum.type =='Neither agree nor disagree',0,0) + if(datum.type ==='Agree',1,0) + if(datum.type ==='Strongly agree',2,0)"),
        case("if(datum.type === 'Disagree' || datum.type === 'Strongly disagree', datum.percentage,0) + if(datum.type === 'Neither agree nor disagree', datum.percentage / 2,0)"),
        case("datum.bin_IMDB_Rating !== null"),
        case("(isDate(datum[\"Year\"]) || (isValid(datum[\"Year\"]) && isFinite(+datum[\"Year\"]))) && isValid(datum[\"Deaths\"]) && isFinite(+datum[\"Deaths\"])"),
        case("year(datum.Date)"),
        case("floor(datum.year / 10)"),
        case("(datum.year % 10) + (month(datum.Date)/12)"),
        case("datum.first_date === datum.scaled_date ? 'first' : datum.last_date === datum.scaled_date ? 'last' : null"),
        case("time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 2, 0, 0, 0, 0)) ? 0 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 3, 0, 0, 0, 0)) ? 1 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 4, 0, 0, 0, 0)) ? 2 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 5, 0, 0, 0, 0)) ? 3 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 6, 0, 0, 0, 0)) ? 4 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 7, 0, 0, 0, 0)) ? 5 : time(datetime(2012, 0, day(datum[\"time\"])+1, 0, 0, 0, 0))===time(datetime(2012, 0, 1, 0, 0, 0, 0)) ? 6 : 7"),
        case("time(datetime(year(datum[\"Release Date\"]), 0, 1, 0, 0, 0, 0)) <= time(datetime(2019, 0, 1, 0, 0, 0, 0))"),
        case("datum.lead === null ? datum.label : datum.lead"),
        case("(datum.label !== 'Begin' && datum.label !== 'End' && datum.amount > 0 ? '+' : '') + datum.amount"),
        case("datum.y - 50"),
        case("((!datum.commonwealth && datum.index % 2) ? -1: 1) * 2 + 95"),
        case("+datum.start + (+datum.end - +datum.start)/2"),
        case("!length(data(\"brush_store\")) || vlSelectionTest(\"brush_store\", datum)"),
        case("year(datum.Year)"),
    )]
    fn test(expr: &str) {
        check_parsing(expr);
        check_expr_supported(expr);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_check_supported {
    use crate::*;
    use vegafusion_core::expression::parser::parse;

    #[rstest(
        expr,
        supported,
        case(
            "isValid(datum[\"average_b\"]) && isFinite(+datum[\"average_b\"])",
            true
        ),
        case("no_such_fn(23)", false),
        case("my_signal[0 + 23]", true),
        case("my_signal[0 + other_signal]", true),
        case("my_signal[datum.col + 'abc']", false)
    )]
    fn test(expr: &str, supported: bool) {
        let expr = parse(expr).unwrap();
        assert_eq!(expr.is_supported(), supported);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
