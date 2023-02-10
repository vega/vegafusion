use datafusion_physical_expr::functions::make_scalar_function;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use vegafusion_common::arrow::compute::no_simd_compare_op_scalar;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
fn make_is_finite_udf() -> ScalarUDF {
    let is_finite = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let is_finite_array = match arg.data_type() {
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
        Ok(Arc::new(is_finite_array) as ArrayRef)
    };
    let is_finite = make_scalar_function(is_finite);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));
    ScalarUDF::new(
        "isfinite",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_finite,
    )
}

lazy_static! {
    pub static ref ISFINITE_UDF: ScalarUDF = make_is_finite_udf();
}
