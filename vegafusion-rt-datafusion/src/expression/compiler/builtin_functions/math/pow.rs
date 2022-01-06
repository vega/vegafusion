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
use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array};
use datafusion::arrow::compute::math_op;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;

/// `pow(value, exponent)`
///
/// Returns value raised to the given exponent. Same as JavaScript’s Math.pow.
///
/// See: https://vega.github.io/vega/docs/expressions/#pow
pub fn make_pow_udf() -> ScalarUDF {
    let pow = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let base = &args[0];
        let exp = &args[1];

        let pow_array = match base.data_type() {
            DataType::Float32 => {
                let base = base.as_any().downcast_ref::<Float32Array>().unwrap();
                let exp = exp.as_any().downcast_ref::<Float32Array>().unwrap();
                let res = math_op(base, exp, |b, e| b.powf(e)).unwrap();
                Arc::new(res) as ArrayRef
            }
            DataType::Float64 => {
                let base = base.as_any().downcast_ref::<Float64Array>().unwrap();
                let exp = exp.as_any().downcast_ref::<Float64Array>().unwrap();
                let res = math_op(base, exp, |b, e| b.powf(e)).unwrap();
                Arc::new(res) as ArrayRef
            }
            _ => {
                // No other type supported by signature
                unreachable!()
            }
        };
        Ok(pow_array)
    };
    let pow = make_scalar_function(pow);
    let return_type: ReturnTypeFunction = Arc::new(move |dtype| Ok(Arc::new(dtype[0].clone())));

    ScalarUDF::new(
        "pow",
        &Signature::uniform(
            2,
            vec![DataType::Float32, DataType::Float64],
            Volatility::Immutable,
        ),
        &return_type,
        &pow,
    )
}
