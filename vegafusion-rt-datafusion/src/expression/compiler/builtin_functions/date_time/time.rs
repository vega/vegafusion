use crate::task_graph::timezone::RuntimeTzConfig;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::Expr;

use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use datafusion::arrow::compute::cast;
use datafusion::common::DFSchema;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{
    lit, ColumnarValue, ExprSchemable, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
    Volatility,
};
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::error::{Result, VegaFusionError};

pub fn time_fn(tz_config: &RuntimeTzConfig, args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    // Validate number of arguments
    if args.len() != 1 {
        return Err(VegaFusionError::compilation(format!(
            "Expected a single argument to time function: received {}",
            args.len()
        )));
    }

    // Extract first and only arg
    let arg = &args[0];

    // Dispatch handling on data type
    let expr = match arg.get_type(schema)? {
        DataType::Timestamp(_, _) => Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_EPOCH_MS).clone()),
            args: vec![arg.clone()],
        },
        DataType::Utf8 => {
            let mut udf_args = vec![lit(tz_config.default_input_tz.to_string())];
            udf_args.extend(Vec::from(args));
            Expr::ScalarUDF {
                fun: Arc::new((*TIMESTAMPTZ_TO_EPOCH_MS).clone()),
                args: vec![Expr::ScalarUDF {
                    fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
                    args: vec![arg.clone(), lit(tz_config.default_input_tz.to_string())],
                }],
            }
        }
        DataType::Int64 => {
            // Keep int argument as-is
            arg.clone()
        }
        dtype if is_numeric_datatype(&dtype) || matches!(dtype, DataType::Boolean) => {
            // Cast other numeric types to Int64
            cast_to(arg.clone(), &DataType::Int64, schema)?
        }
        dtype => {
            return Err(VegaFusionError::internal(format!(
                "Invalid argument type to time function: {:?}",
                dtype
            )))
        }
    };

    Ok(expr)
}

pub fn make_timestamptz_to_epoch_ms() -> ScalarUDF {
    let time_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        // cast timestamp millis to Int64
        let result_array = cast(&data_array, &DataType::Int64)?;

        // maybe back to scalar
        if result_array.len() != 1 {
            Ok(ColumnarValue::Array(result_array))
        } else {
            ScalarValue::try_from_array(&result_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    let signature: Signature = Signature::exact(
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "timestamptz_to_epoch_ms",
        &signature,
        &return_type,
        &time_fn,
    )
}

lazy_static! {
    pub static ref TIMESTAMPTZ_TO_EPOCH_MS: ScalarUDF = make_timestamptz_to_epoch_ms();
}
