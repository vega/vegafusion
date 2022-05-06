/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array};
use datafusion::arrow::compute::math_op;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
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
