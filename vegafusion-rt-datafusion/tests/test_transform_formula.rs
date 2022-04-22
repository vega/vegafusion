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
use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;

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
