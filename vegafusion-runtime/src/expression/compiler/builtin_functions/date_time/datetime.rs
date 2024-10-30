use crate::datafusion::udfs::datetime::make_timestamptz::make_timestamptz;
use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::utils::{from_epoch_millis, str_to_timestamp};
use datafusion_expr::{lit, Expr, ExprSchemable};
use std::ops::Add;
use std::str::FromStr;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype, is_string_datatype};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

pub fn to_date_transform(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    // Datetime from string or integer in milliseconds
    let arg = args[0].clone();
    let dtype = arg
        .get_type(schema)
        .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

    if is_string_datatype(&dtype) {
        let default_input_tz = if args.len() == 2 {
            // Second argument is an override local timezone string
            let input_tz_expr = &args[1];
            if let Expr::Literal(ScalarValue::Utf8(Some(input_tz_str))) = input_tz_expr {
                if input_tz_str == "local" {
                    tz_config.local_tz
                } else {
                    chrono_tz::Tz::from_str(input_tz_str)
                        .ok()
                        .with_context(|| format!("Failed to parse {input_tz_str} as a timezone"))?
                }
            } else {
                return Err(VegaFusionError::parse(
                    "Second argument to toDate must be a timezone string",
                ));
            }
        } else {
            tz_config.default_input_tz
        };

        let ex = str_to_timestamp(arg, &default_input_tz.to_string(), schema, None)?;
        Ok(ex)
    } else if is_numeric_datatype(&dtype) {
        from_epoch_millis(arg, schema)
    } else {
        Ok(arg)
    }
}

pub fn datetime_transform_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    if args.len() == 1 {
        // Datetime from string or integer in milliseconds
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        if is_string_datatype(&dtype) {
            let default_input_tz_str = tz_config.default_input_tz.to_string();
            str_to_timestamp(arg, &default_input_tz_str, schema, None)
        } else {
            from_epoch_millis(arg, schema)
        }
    } else {
        let udf_args =
            extract_datetime_component_args(args, &tz_config.default_input_tz.to_string(), schema)?;

        Ok(make_timestamptz(
            udf_args[0].clone(),             // year
            udf_args[1].clone().add(lit(1)), // month (arg 1-based, vega uses zero-based)
            udf_args[2].clone(),             // day
            udf_args[3].clone(),             // hour
            udf_args[4].clone(),             // minute
            udf_args[5].clone(),             // second
            udf_args[6].clone(),             // millisecond
            &tz_config.local_tz.to_string(),
        ))
    }
}

pub fn make_datetime_components_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let udf_args =
        extract_datetime_component_args(args, &tz_config.default_input_tz.to_string(), schema)?;

    Ok(make_timestamptz(
        udf_args[0].clone(),             // year
        udf_args[1].clone().add(lit(1)), // month (arg 1-based, vega uses zero-based)
        udf_args[2].clone(),             // day
        udf_args[3].clone(),             // hour
        udf_args[4].clone(),             // minute
        udf_args[5].clone(),             // second
        udf_args[6].clone(),             // millisecond
        "UTC",
    ))
}

fn extract_datetime_component_args(
    args: &[Expr],
    tz_str: &str,
    schema: &DFSchema,
) -> Result<Vec<Expr>> {
    // Cast numeric args to integers
    let mut result_args: Vec<_> = args
        .iter()
        .map(|arg| cast_to(arg.clone(), &DataType::Int64, schema))
        .collect::<Result<Vec<_>>>()?;

    // Pad unspecified args
    if result_args.len() < 2 {
        // default to 1st (zero-based) month of the year
        result_args.push(lit(0i64))
    }

    if result_args.len() < 3 {
        // default to 1st of the month
        result_args.push(lit(1i64))
    }

    // Remaining args (hour, minute, second, millisecond) default to zero
    let num_args = result_args.len();
    for _ in num_args..7 {
        result_args.push(lit(0i64));
    }

    result_args.push(lit(tz_str));

    Ok(result_args)
}
