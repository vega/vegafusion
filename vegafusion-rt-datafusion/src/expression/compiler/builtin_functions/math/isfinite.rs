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
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use datafusion::arrow::compute::no_simd_compare_op_scalar;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
pub fn make_is_finite_udf() -> ScalarUDF {
    let is_finite = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let is_nan_array = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                no_simd_compare_op_scalar(array, f32::NAN, |a, _| a.is_finite()).unwrap()
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                no_simd_compare_op_scalar(array, f64::NAN, |a, _| a.is_finite()).unwrap()
            }
            _ => {
                // No other type can be non-finite
                BooleanArray::from(vec![true; arg.len()])
            }
        };
        Ok(Arc::new(is_nan_array) as ArrayRef)
    };
    let is_finite = make_scalar_function(is_finite);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));
    ScalarUDF::new(
        "isFinite",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_finite,
    )
}
