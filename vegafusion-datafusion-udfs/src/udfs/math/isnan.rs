use datafusion_physical_expr::functions::make_scalar_function;
use std::sync::Arc;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, BooleanArray, Float32Array, Float64Array},
        datatypes::DataType,
    },
    datafusion_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility},
};

/// isNaN(value)
///
/// Returns true if value is not a number. Same as JavaScript’s Number.isNaN.
///
/// See: https://vega.github.io/vega/docs/expressions/#isNaN
fn make_is_nan_udf() -> ScalarUDF {
    let is_nan = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let is_nan_array = match arg.data_type() {
            DataType::Float32 => {
                let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                BooleanArray::from_unary(array, |a| a.is_nan())
            }
            DataType::Float64 => {
                let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                BooleanArray::from_unary(array, |a| a.is_nan())
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
        "isnan",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_nan,
    )
}

lazy_static! {
    pub static ref ISNAN_UDF: ScalarUDF = make_is_nan_udf();
}
