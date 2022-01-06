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
