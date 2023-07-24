#[macro_use]
extern crate lazy_static;

mod util;

use datafusion_common::ScalarValue;
use serde_json::json;
use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_runtime::expression::compiler::config::CompilationConfig;

#[test]
fn test_formula_valid() {
    let dataset = vega_json_dataset("penguins");
    let formula_spec = FormulaTransformSpec {
        expr: "isValid(datum.Sex) && datum.Sex != '.'".to_string(),
        as_: "it_is_valid".to_string(),
        extra: Default::default(),
    };
    let transform_specs = vec![TransformSpec::Formula(formula_spec)];

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

#[test]
fn test_formula_signal_expression() {
    let dataset = vega_json_dataset("penguins");

    // Apply filter transform pipeline stage to remove Nulls and keep flipper lengths less than
    // threshold
    let formula_spec = FormulaTransformSpec {
        expr: "if(isValid(datum.Sex) && isValid(datum['Flipper Length (mm)']) && datum['Flipper Length (mm)'] > threshold, datum['Flipper Length (mm)'] / 10, -1.0)"
            .to_string(),
        as_: "flipper_feature".to_string(),
        extra: Default::default(),
    };
    let transform_specs = vec![TransformSpec::Formula(formula_spec)];

    let eq_config = Default::default();

    let threshold = 190.0;
    let comp_config = CompilationConfig {
        signal_scope: vec![("threshold".to_string(), ScalarValue::from(threshold))]
            .into_iter()
            .collect(),
        ..Default::default()
    };

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

#[test]
fn test_formula_indexof() {
    let dataset = vega_json_dataset("penguins");
    let transform_specs = vec![
        TransformSpec::Formula(FormulaTransformSpec {
            expr: "indexof(['Torgersen', 'Dream'], datum.Island)".to_string(),
            as_: "indexof_string".to_string(),
            extra: Default::default(),
        }),
        TransformSpec::Formula(FormulaTransformSpec {
            expr: "indexof([3300, 4300, 3700, 4350], datum['Body Mass (g)'])".to_string(),
            as_: "indexof_int".to_string(),
            extra: Default::default(),
        }),
    ];

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

#[test]
fn test_formula_bitwise() {
    let dataset = vega_json_dataset("penguins");

    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "formula",
            "expr": "datum['Body Mass (g)'] & 15 || 0",
            "as": "bitwise_and"
        },
        {
            "type": "formula",
            "expr": "datum['Body Mass (g)'] ^ 15 || 0",
            "as": "bitwise_xor"
        },
        {
            "type": "formula",
            "expr": "datum['Body Mass (g)'] | 15 || 0",
            "as": "bitwise_or"
        },
        {
            "type": "formula",
            "expr": "datum['Body Mass (g)'] << 3 || 0",
            "as": "bitwise_lshift"
        },
        {
            "type": "formula",
            "expr": "datum['Body Mass (g)'] >> 2 || 0",
            "as": "bitwise_rshift"
        }
    ]))
    .unwrap();

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

#[test]
fn test_formula_recursive_ternary_doesnt_overflow_stack() {
    let dataset = vega_json_dataset("penguins");

    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "formula",
            "as": "nested_ternary",
            "expr": "\
            datum['Island'] == 'A' ? 3: \
            datum['Island'] == 'B' ? 4: \
            datum['Island'] == 'C' ? 5: \
            datum['Island'] == 'Dream' ? 6: \
            datum['Island'] == 'E' ? 7: \
            datum['Island'] == 'F' ? 8: \
            datum['Island'] == 'G' ? 9: \
            datum['Island'] == 'H' ? 10: \
            datum['Island'] == 'I' ? 11: \
            datum['Island'] == 'J' ? 12: \
            datum['Island'] == 'K' ? 13: \
            datum['Island'] == 'L' ? 14: \
            datum['Island'] == 'M' ? 15: \
            datum['Island'] == 'N' ? 16: \
            datum['Island'] == 'O' ? 17: \
            datum['Island'] == 'P' ? 18: \
            datum['Island'] == 'Q' ? 19: \
            datum['Island'] == 'R' ? 20: \
            datum['Island'] == 'S' ? 21: \
            datum['Island'] == 'Torgersen' ? 22: \
            datum['Island'] == 'U' ? 23: \
            datum['Island'] == 'V' ? 24: \
            datum['Island'] == 'W' ? 25: \
            datum['Island'] == 'X' ? 26: \
            datum['Island'] == 'Y' ? 27: \
            datum['Island'] == 'Z' ? 28: \
            datum['Island'] == 'AA' ? 29: \
            datum['Island'] == 'Biscoe' ? 30: \
            datum['Island'] == 'CC' ? 31: \
            datum['Island'] == 'DD' ? 32: \
            datum['Island'] == 'EE' ? 33: \
            datum['Island'] == 'FF' ? 34: \
            datum['Island'] == 'GG' ? 35: \
            datum['Island'] == 'HH' ? 36: \
            datum['Island'] == 'II' ? 37: \
            datum['Island'] == 'JJ' ? 38: \
            datum['Island'] == 'KK' ? 39: \
            datum['Island'] == 'LL' ? 40: \
            datum['Island'] == 'MM' ? 41: \
            datum['Island'] == 'NN' ? 42: \
            datum['Island'] == 'OO' ? 43: \
            datum['Island'] == 'PP' ? 44: \
            datum['Island'] == 'QQ' ? 45: \
            datum['Island'] == 'RR' ? 46: \
            datum['Island'] == 'SS' ? 47: \
            datum['Island'] == 'TT' ? 48: \
            datum['Island'] == 'UU' ? 49: \
            datum['Island'] == 'VV' ? 50: \
            datum['Island'] == 'WW' ? 51: \
            datum['Island'] == 'XX' ? 52: \
            datum['Island'] == 'YY' ? 53: \
            datum['Island'] == 'ZZ' ? 54: \
            datum['Island'] == 'AAA' ? 55: \
            datum['Island'] == 'BBB' ? 56: \
            datum['Island'] == 'CCC' ? 57: \
            datum['Island'] == 'DDD' ? 58: \
            datum['Island'] == 'EEE' ? 59: \
            datum['Island'] == 'FFF' ? 60: \
            datum['Island'] == 'GGG' ? 61: \
            datum['Island'] == 'HHH' ? 62: \
            datum['Island'] == 'III' ? 63: \
            datum['Island'] == 'JJJ' ? 64: \
            datum['Island'] == 'KKK' ? 65: \
            datum['Island'] == 'LLL' ? 66: \
            datum['Island'] == 'MMM' ? 67: \
            datum['Island'] == 'NNN' ? 68: \
            datum['Island'] == 'OOO' ? 69: \
            datum['Island'] == 'PPP' ? 70: \
            datum['Island'] == 'QQQ' ? 71: \
            datum['Island'] == 'RRR' ? 72: \
            datum['Island'] == 'SSS' ? 73: \
            datum['Island'] == 'TTT' ? 74: \
            datum['Island'] == 'UUU' ? 75: \
            datum['Island'] == 'VVV' ? 76: \
            datum['Island'] == 'WWW' ? 77: \
            datum['Island'] == 'XXX' ? 78: \
            datum['Island'] == 'YYY' ? 79: \
            datum['Island'] == 'ZZZ' ? 80: \
            81
            ",
        },
    ]))
    .unwrap();

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}
