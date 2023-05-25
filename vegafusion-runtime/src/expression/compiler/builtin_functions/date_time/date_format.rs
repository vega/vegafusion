use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::{expr, lit, Expr, ExprSchemable};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};
use vegafusion_core::arrow::datatypes::TimeUnit;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_utc_timestamp::EPOCH_MS_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::format_timestamp::FORMAT_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::from_utc_timestamp::FROM_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_str::UTC_TIMESTAMP_TO_STR_UDF;

pub fn time_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;

    // Handle format timezone override
    let format_tz_str = if args.len() >= 3 {
        // Second argument is a an override local timezone string
        let format_tz_expr = &args[2];
        if let Expr::Literal(ScalarValue::Utf8(Some(format_tz_str))) = format_tz_expr {
            format_tz_str.clone()
        } else {
            return Err(VegaFusionError::parse(
                "Third argument to timeFormat must be a timezone string",
            ));
        }
    } else {
        tz_config.local_tz.to_string()
    };

    let mut timestamptz_expr =
        to_timestamptz_expr(&args[0], schema, &tz_config.default_input_tz.to_string())?;

    if format_str == "%Y-%m-%dT%H:%M:%S.%L" {
        // Special case for ISO-8601 format with milliseconds. The UTC_TIMESTAMP_TO_STR_UDF
        // is compatible with more SQL dialects, so we want to use it if possible
        let udf_args = vec![timestamptz_expr, lit(&format_tz_str)];
        Ok(Expr::ScalarUDF(expr::ScalarUDF {
            fun: Arc::new((*UTC_TIMESTAMP_TO_STR_UDF).clone()),
            args: udf_args,
        }))
    } else {
        // General case
        if format_tz_str.to_ascii_lowercase() != "utc" {
            timestamptz_expr = Expr::ScalarUDF(expr::ScalarUDF {
                fun: Arc::new((*FROM_UTC_TIMESTAMP_UDF).clone()),
                args: vec![timestamptz_expr, lit(format_tz_str)],
            })
        }

        let udf_args = vec![timestamptz_expr, lit(format_str)];

        Ok(Expr::ScalarUDF(expr::ScalarUDF {
            fun: Arc::new((*FORMAT_TIMESTAMP_UDF).clone()),
            args: udf_args,
        }))
    }
}

pub fn utc_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;
    let timestamptz_expr =
        to_timestamptz_expr(&args[0], schema, &tz_config.default_input_tz.to_string())?;

    if format_str == "%Y-%m-%dT%H:%M:%S.%L" {
        // Special case for ISO-8601 format with milliseconds. The UTC_TIMESTAMP_TO_STR_UDF
        // is compatible with more SQL dialects, so we want to use it if possible
        let udf_args = vec![timestamptz_expr, lit("UTC")];
        Ok(Expr::ScalarUDF(expr::ScalarUDF {
            fun: Arc::new((*UTC_TIMESTAMP_TO_STR_UDF).clone()),
            args: udf_args,
        }))
    } else {
        // General case
        let udf_args = vec![timestamptz_expr, lit(format_str)];
        Ok(Expr::ScalarUDF(expr::ScalarUDF {
            fun: Arc::new((*FORMAT_TIMESTAMP_UDF).clone()),
            args: udf_args,
        }))
    }
}

fn to_timestamptz_expr(arg: &Expr, schema: &DFSchema, default_input_tz: &str) -> Result<Expr> {
    Ok(match arg.get_type(schema)? {
        DataType::Date32 => Expr::Cast(expr::Cast {
            expr: Box::new(arg.clone()),
            data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
        }),
        DataType::Date64 => Expr::Cast(expr::Cast {
            expr: Box::new(arg.clone()),
            data_type: DataType::Timestamp(TimeUnit::Millisecond, None),
        }),
        DataType::Timestamp(_, _) => arg.clone(),
        DataType::Utf8 => Expr::ScalarUDF(expr::ScalarUDF {
            fun: Arc::new((*STR_TO_UTC_TIMESTAMP_UDF).clone()),
            args: vec![arg.clone(), lit(default_input_tz)],
        }),
        DataType::Null => arg.clone(),
        dtype if is_numeric_datatype(&dtype) || matches!(dtype, DataType::Boolean) => {
            Expr::ScalarUDF(expr::ScalarUDF {
                fun: Arc::new((*EPOCH_MS_TO_UTC_TIMESTAMP_UDF).clone()),
                args: vec![cast_to(arg.clone(), &DataType::Int64, schema)?],
            })
        }
        dtype => {
            return Err(VegaFusionError::internal(format!(
                "Invalid argument type to timeFormat function: {dtype:?}"
            )))
        }
    })
}

pub fn extract_format_str(args: &[Expr]) -> Result<String> {
    let format_str = if args.len() >= 2 {
        let format_arg = &args[1];
        match format_arg {
            Expr::Literal(ScalarValue::Utf8(Some(format_str))) => Ok(format_str.clone()),
            _ => {
                return Err(VegaFusionError::compilation(
                    "the second argument to the timeFormat function must be a literal string",
                ))
            }
        }
    } else if args.len() == 1 {
        Ok("%I:%M".to_string())
    } else {
        Err(VegaFusionError::compilation(
            "the timeFormat function must have at least one argument",
        ))
    }?;
    Ok(format_str)
}
