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
use serde_json::Value;
use vegafusion_core::data::table::VegaFusionTable;

pub fn vega_json_dataset(name: &str) -> VegaFusionTable {
    // Remove trailing .json if present because we'll add it below
    let name = match name.strip_suffix(".json") {
        None => name,
        Some(name) => name,
    };

    // Fetch dataset from vega-datasets repository
    let body = reqwest::blocking::get(format!(
        "https://raw.githubusercontent.com/vega/vega-datasets/master/data/{}.json",
        name
    ))
    .unwrap()
    .text()
    .unwrap();
    let json_value: Value = serde_json::from_str(&body).unwrap();

    VegaFusionTable::from_json(&json_value, 1024).unwrap()
}
