use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::{lit, Expr, ExprSchemable};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_datafusion_udfs::udfs::datetime::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timestamptz_to_epoch::TIMESTAMPTZ_TO_EPOCH_MS;

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
                "Invalid argument type to time function: {dtype:?}"
            )))
        }
    };

    Ok(expr)
}
