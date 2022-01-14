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
use std::collections::HashSet;

lazy_static! {
    pub static ref BUILT_IN_SIGNALS: HashSet<&'static str> =
        vec!["width", "height", "padding", "autosize", "background"]
            .into_iter()
            .collect();

    pub static ref IMPLICIT_VARS: HashSet<&'static str> =
        vec!["datum", "event"]
            .into_iter()
            .collect();

    pub static ref ALL_DATA_FNS: HashSet<&'static str> = vec![
        "data", "indata", "vlSelectionTest", "vlSelectionResolve", "modify"
    ]
    .into_iter()
    .collect();

    pub static ref ALL_SCALE_FNS: HashSet<&'static str> = vec![
        "scale",
        "invert",
        "domain",
        "range",
        "bandwidth",
        "gradient",
    ]
    .into_iter()
    .collect();


    pub static ref SUPPORTED_DATA_FNS: HashSet<&'static str> = vec![
        "data", "vlSelectionTest", "vlSelectionResolve"
    ]
    .into_iter()
    .collect();

    pub static ref SUPPORTED_SCALE_FNS: HashSet<&'static str> = Default::default();

    pub static ref SUPPORTED_EXPRESSION_FNS: HashSet<&'static str> = vec![
        // Math
        "abs", "acos", "asin", "atan", "ceil", "cos", "exp", "floor", "round", "sqrt", "tan",
        "log", "pow",

        // Type checking
        "isNaN", "isFinite", "isValid", "isDate",

        // Array
        "length", "span",

        // Datetime
        "year", "quarter", "month", "day", "date", "dayofyear", "hours", "minutes", "seconds",
        "utcyear", "utcquarter", "utcmonth", "utcday", "utcdate", "utcdayofyear",
        "utchours", "utcminutes", "utcseconds", "datetime", "utc", "time",

        // Conversion
        "toBoolean", "toDate", "toNumber", "toString",

        // Control flow
        "if",
    ]
    .into_iter()
    .collect();

    pub static ref ALL_EXPRESSION_CONSTANTS: HashSet<&'static str> = vec![
        "NaN", "E", "LN2", "LOG2E", "LOG10E", "MAX_VALUE", "MIN_VALUE", "PI", "SQRT1_2",  "SQRT2"
    ]
    .into_iter()
    .collect();
}
