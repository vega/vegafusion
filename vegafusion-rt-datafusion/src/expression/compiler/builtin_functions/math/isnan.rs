/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use datafusion::arrow::compute::no_simd_compare_op_scalar;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;

/// isNaN(value)
///
/// Returns true if value is not a number. Same as JavaScript’s Number.isNaN.
///
/// See: https://vega.github.io/vega/docs/expressions/#isNaN
pub fn make_is_nan_udf() -> ScalarUDF {
    let is_nan = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let is_nan_array = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                no_simd_compare_op_scalar(array, f32::NAN, |a, _| a.is_nan()).unwrap()
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                no_simd_compare_op_scalar(array, f64::NAN, |a, _| a.is_nan()).unwrap()
            }
            _ => {
                // No other type can be NaN
                BooleanArray::from(vec![false; arg.len()])
            }
        };
        Ok(Arc::new(is_nan_array) as ArrayRef)
    };
    let is_nan = make_scalar_function(is_nan);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));
    ScalarUDF::new(
        "isNaN",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_nan,
    )
}
