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
use crate::util::check::check_scalar_evaluation;
use rstest::*;
use serde_json::{json, Value};
use util::check::check_transform_evaluation;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;

fn make_brush_r(ranges: &Vec<Vec<(&str, &str, [f64; 2])>>, typ: &str) -> VegaFusionTable {
    let mut rows: Vec<Value> = Vec::new();
    for (i, row_ranges) in ranges.iter().enumerate() {
        let mut field_elements: Vec<Value> = Vec::new();
        let mut value_elements: Vec<Value> = Vec::new();

        for (field, channel, range) in row_ranges {
            field_elements.push(json!({
                "field": field.to_string(),
                "channel": channel.to_string(),
                "type": typ,
            }));

            value_elements.push(json!([range[0], range[1]]))
        }

        rows.push(json!({
            "unit": format!("unit{}", i),
            "fields": Value::Array(field_elements),
            "values": Value::Array(value_elements),
        }));
    }
    VegaFusionTable::from_json(&Value::Array(rows), 1024).unwrap()
}

fn make_brush_e_single(field: &str, values: &[f64]) -> VegaFusionTable {
    let mut rows: Vec<Value> = Vec::new();

    for (i, val) in values.iter().enumerate() {
        let field_element = json!({
            "type": "E",
            "field": field,
        });
        let value_element = json!(*val);

        rows.push(json!({
            "unit": format!("unit{}", i),
            "fields": Value::Array(vec![field_element]),
            "values": Value::Array(vec![value_element]),
        }));
    }

    VegaFusionTable::from_json(&Value::Array(rows), 1024).unwrap()
}

fn make_brush_e_str(ranges: &Vec<Vec<(&str, &str, Vec<&str>)>>) -> VegaFusionTable {
    let mut rows: Vec<Value> = Vec::new();
    for (i, row_ranges) in ranges.iter().enumerate() {
        let mut field_elements: Vec<Value> = Vec::new();
        let mut value_elements: Vec<Value> = Vec::new();

        for (field, channel, values) in row_ranges {
            field_elements.push(json!({
                "field": field.to_string(),
                "channel": channel.to_string(),
                "type": "E",
            }));

            value_elements.push(json!(*values))
        }

        rows.push(json!({
            "unit": format!("unit{}", i),
            "fields": Value::Array(field_elements),
            "values": Value::Array(value_elements),
        }));
    }

    VegaFusionTable::from_json(&Value::Array(rows), 1024).unwrap()
}

fn datum() -> VegaFusionTable {
    let json_value = json!([
        {"colA": 1.0, "colB": 10.0, "colC": 100.0, "colB_str": "10", "__vgsid__": 1, "cat1": "AA", "cat2": "aa"},
        {"colA": 2.0, "colB": 9.0, "colC": 200.0, "colB_str": "9", "__vgsid__": 2, "cat1": "AA", "cat2": "bb"},
        {"colA": 3.0, "colB": 8.0, "colC": 300.0, "colB_str": "8", "__vgsid__": 3, "cat1": "AA", "cat2": "aa"},
        {"colA": 4.0, "colB": 7.0, "colC": 400.0, "colB_str": "7", "__vgsid__": 4, "cat1": "AA", "cat2": "bb"},
        {"colA": 5.0, "colB": 6.0, "colC": 500.0, "colB_str": "6", "__vgsid__": 5, "cat1": "BB", "cat2": "aa"},
        {"colA": 6.0, "colB": 5.0, "colC": 600.0, "colB_str": "5", "__vgsid__": 6, "cat1": "BB", "cat2": "bb"},
        {"colA": 7.0, "colB": 4.0, "colC": 700.0, "colB_str": "4", "__vgsid__": 7, "cat1": "BB", "cat2": "aa"},
    ]);
    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}

pub fn check_vl_selection_test(
    selection_expr: &str,
    brush_dataset: VegaFusionTable,
    dataset: &VegaFusionTable,
) {
    let formula_spec = FormulaTransformSpec {
        expr: selection_expr.to_string(),
        as_: "it_is_selected".to_string(),
        extra: Default::default(),
    };

    let transform_specs = vec![TransformSpec::Formula(formula_spec)];

    let config = CompilationConfig {
        data_scope: vec![("brush".to_string(), brush_dataset)]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let eq_config = Default::default();

    check_transform_evaluation(dataset, transform_specs.as_slice(), &config, &eq_config);
}

pub fn check_vl_selection_resolve(selection_expr: &str, brush_dataset: VegaFusionTable) {
    let config = CompilationConfig {
        data_scope: vec![("brush".to_string(), brush_dataset)]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    check_scalar_evaluation(selection_expr, &config);
}

mod test_vl_selection_test_r {
    use crate::*;

    #[rstest(
        brush_data, typ, op,
        case(vec![vec![("colA", "x", [2.0, 5.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 2.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [2.0, 5.0])]], "R-E", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 2.0])]], "R-E", "intersect"),
        case(vec![vec![("colA", "x", [2.0, 5.0])]], "R-LE", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 2.0])]], "R-LE", "intersect"),
        case(vec![vec![("colA", "x", [2.0, 5.0])]], "R-RE", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 2.0])]], "R-RE", "intersect"),
        case(vec![vec![("colA", "x", [2.0, 10.0]), ("colB", "y", [0.0, 6.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [10.0, 2.0]), ("colB", "y", [6.0, 0.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 10.0]), ("colB", "y", [8.0, 10.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [10.0, 5.0]), ("colB", "y", [10.0, 8.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [10.0, 2.0]), ("colB_str", "y", [8.0, 10.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 10.0])], vec![("colB", "y", [8.0, 10.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [10.0, 5.0])], vec![("colB", "y", [10.0, 8.0])]], "R", "intersect"),
        case(vec![vec![("colA", "x", [5.0, 10.0])], vec![("colB", "y", [8.0, 10.0])]], "R", "union"),
        case(vec![vec![("colA", "x", [10.0, 5.0])], vec![("colB", "y", [10.0, 8.0])]], "R", "union"),
        case(vec![vec![("colA", "x", [10.0, 5.0])], vec![("colB_str", "y", [10.0, 8.0])]], "R", "union")
    )]
    fn test(brush_data: Vec<Vec<(&str, &str, [f64; 2])>>, typ: &str, op: &str) {
        let brush = make_brush_r(&brush_data, typ);
        let test_expr = format!("vlSelectionTest('brush', datum, '{}')", op);
        println!("{}", test_expr);
        check_vl_selection_test(&test_expr, brush.clone(), &datum());

        let resolve_expr = format!("vlSelectionResolve('brush', '{}')", op);
        check_vl_selection_resolve(&resolve_expr, brush);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_vl_selection_test_e_single {
    use crate::*;

    #[rstest(
        points, op,
        case(&[1.0, 2.0, 3.0], "union")
    )]
    fn test(points: &[f64], op: &str) {
        let brush = make_brush_e_single("__vgsid__", points);
        let expr = format!("vlSelectionTest('brush', datum, '{}')", op);
        check_vl_selection_test(&expr, brush.clone(), &datum());

        let resolve_expr = format!("vlSelectionResolve('brush', '{}')", op);
        check_vl_selection_resolve(&resolve_expr, brush);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

mod test_vl_selection_test_e_multi {
    use crate::*;

    #[rstest(
        brush_data, op,
        case(vec![vec![("cat1", "x", vec!["AA"])]], "union"),
        case(vec![vec![("cat2", "y", vec!["aa"])]], "intersect"),
        case(vec![vec![("cat1", "x", vec!["AA"]), ("cat2", "y", vec!["aa"])]], "intersect"),
        case(vec![vec![("cat1", "x", vec!["AA"])], vec![("cat2", "y", vec!["aa"])]], "intersect"),
        case(vec![vec![("cat1", "x", vec!["AA"])], vec![("cat2", "y", vec!["aa"])]], "union"),
        case(vec![vec![("cat1", "x", vec!["AA"])], vec![("cat2", "y", vec!["aa", "bb"])]], "intersect"),
    )]
    fn test(brush_data: Vec<Vec<(&str, &str, Vec<&str>)>>, op: &str) {
        let brush = make_brush_e_str(&brush_data);
        let expr = format!("vlSelectionTest('brush', datum, '{}')", op);
        check_vl_selection_test(&expr, brush.clone(), &datum());

        let resolve_expr = format!("vlSelectionResolve('brush', '{}')", op);
        check_vl_selection_resolve(&resolve_expr, brush);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
