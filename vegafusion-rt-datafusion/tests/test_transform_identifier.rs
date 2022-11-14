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
use vegafusion_core::spec::transform::identifier::IdentifierTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;

#[test]
fn test_formula_valid() {
    let dataset = vega_json_dataset("penguins");
    let tx_spec = IdentifierTransformSpec {
        as_: "id".to_string(),
        extra: Default::default(),
    };

    let transform_specs = vec![TransformSpec::Identifier(tx_spec)];

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}
