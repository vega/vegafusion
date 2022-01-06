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
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::is_not_null;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;

/// `isValid(value)`
///
/// Returns true if value is not null, undefined, or NaN, false otherwise.
///
/// Note: Current implementation does not consider NaN values invalid
///
/// See: https://vega.github.io/vega/docs/expressions/#isValid
pub fn make_is_valid_udf() -> ScalarUDF {
    let is_valid = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        let result = is_not_null(arg.as_ref()).unwrap();
        Ok(Arc::new(result) as ArrayRef)
    };
    let is_valid = make_scalar_function(is_valid);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));
    ScalarUDF::new(
        "isValid",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_valid,
    )
}
