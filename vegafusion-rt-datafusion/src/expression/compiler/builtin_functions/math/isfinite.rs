use datafusion::arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use datafusion::arrow::compute::no_simd_compare_op_scalar;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DFSchema;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{lit, Expr, ExprSchemable, ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
pub fn is_finite_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

        Ok(match dtype {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                let is_finite_udf = make_is_finite_udf();
                Expr::ScalarUDF {
                    fun: Arc::new(is_finite_udf),
                    args: vec![arg],
                }
            }
            _ => {
                // Non-numeric types cannot be non-finite
                lit(true)
            }
        })
    } else {
        Err(VegaFusionError::parse(format!(
            "isValid requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}

pub fn make_is_finite_udf() -> ScalarUDF {
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
