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

use vegafusion_core::spec::transform::collect::CollectTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::spec::values::{
    CompareSpec, SortOrderOrList, SortOrderSpec, StringOrStringList,
};

#[test]
fn test_collect_multi() {
    let dataset = vega_json_dataset("penguins");

    // Apply collect transform
    let collect_spec = CollectTransformSpec {
        sort: CompareSpec {
            field: StringOrStringList::StringList(vec![
                "Sex".to_string(),
                "Species".to_string(),
                "Beak Depth (mm)".to_string(),
                "Beak Length (mm)".to_string(),
                "Flipper Length (mm)".to_string(),
            ]),
            order: Some(SortOrderOrList::SortOrderList(vec![
                SortOrderSpec::Ascending,
                SortOrderSpec::Descending,
                SortOrderSpec::Descending,
                SortOrderSpec::Ascending,
                SortOrderSpec::Descending,
            ])),
        },
        extra: Default::default(),
    };

    let transform_specs = vec![TransformSpec::Collect(collect_spec)];

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
