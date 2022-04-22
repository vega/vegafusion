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

use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use vegafusion_core::spec::transform::extent::ExtentTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;

#[test]
fn test_extent_signal() {
    let dataset = vega_json_dataset("penguins");

    let extent_spec = ExtentTransformSpec {
        field: "Beak Length (mm)".to_string(),
        signal: Some("my_extent".to_string()),
        extra: Default::default(),
    };
    let transform_specs = vec![TransformSpec::Extent(extent_spec)];

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
fn test_extent_no_signal() {
    // Make sure nothing breaks when no signal is defined

    let dataset = vega_json_dataset("penguins");

    let extent_spec = ExtentTransformSpec {
        field: "Beak Length (mm)".to_string(),
        signal: None,
        extra: Default::default(),
    };
    let transform_specs = vec![TransformSpec::Extent(extent_spec)];

    let comp_config = Default::default();
    let eq_config = Default::default();

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}
