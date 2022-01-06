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
use util::equality::TablesEqualConfig;

use vegafusion_core::spec::transform::bin::{BinExtent, BinTransformSpec};
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::{Field, SignalExpressionSpec};

#[test]
fn test_bin() {
    let dataset = vega_json_dataset("penguins");

    let bin_spec = BinTransformSpec {
        field: Field::String("Body Mass (g)".to_string()),
        extent: BinExtent::Signal(SignalExpressionSpec {
            signal: "[2000.0 + 1000, 4000 + 1000]".to_string(),
        }),
        signal: Some("my_bins".to_string()),
        as_: None,
        anchor: None,
        maxbins: None,
        base: None,
        step: None,
        steps: None,
        span: None,
        minstep: None,
        divide: None,
        nice: None,
        extra: Default::default(),
    };

    let transform_specs = vec![TransformSpec::Bin(Box::new(bin_spec))];

    let comp_config = Default::default();
    let eq_config = TablesEqualConfig {
        row_order: true,
        ..Default::default()
    };

    check_transform_evaluation(
        &dataset,
        transform_specs.as_slice(),
        &comp_config,
        &eq_config,
    );
}

// Note: Query results in an error that looks like it might be a bug in DataFusion
//
// "No field named '<unqualified>.df.Beak Length (mm)'. Valid fields are 'df.Species', 'df.Island',
// 'df.Beak Length (mm)', 'df.Beak Depth (mm)', 'df.Flipper Length (mm)', 'df.Body Mass (g)', 'df.Sex',
// 'bin0', 'bin1'."
//
// #[test]
// fn test_bin_infs() {
//     let dataset = vega_json_dataset("penguins");
//
//     let bin_spec = BinTransformSpec {
//         field: Field::String("Body Mass (g)".to_string()),
//         extent: BinExtent::Signal(SignalExpressionSpec {
//             signal: "[2000.0 + 1000, 4000 + 1000]".to_string(),
//         }),
//         signal: Some("my_bins".to_string()),
//         as_: None,
//         anchor: None,
//         maxbins: None,
//         base: None,
//         step: None,
//         steps: None,
//         span: None,
//         minstep: None,
//         divide: None,
//         nice: None,
//         extra: Default::default(),
//     };
//
//     let formula_spec = FormulaTransformSpec {
//         expr: "if(datum.bin0 <= -1/0, -1, if(datum.bin0 >= 1/0, 1, 0))".to_string(),
//         // expr: "datum['Body Mass (g)']".to_string(),
//         as_: "inf_sign".to_string(),
//         extra: Default::default()
//     };
//
//     let transform_specs = vec![
//         TransformSpec::Bin(bin_spec),
//         TransformSpec::Formula(formula_spec),
//     ];
//
//     let comp_config = Default::default();
//     let eq_config = TablesEqualConfig {
//         row_order: true,
//         ..Default::default()
//     };
//
//     check_transform_evaluation(
//         &dataset,
//         transform_specs.as_slice(),
//         &comp_config,
//         &eq_config,
//     );
// }
