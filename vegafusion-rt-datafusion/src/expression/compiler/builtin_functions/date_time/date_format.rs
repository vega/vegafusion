/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::task_graph::timezone::RuntimeTzConfig;
use chrono::TimeZone;
use chrono::{DateTime, NaiveDateTime};
use datafusion::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_plan::{DFSchema, Expr};

use crate::expression::compiler::builtin_functions::date_time::epoch_to_timestamptz::EPOCH_MS_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{
    lit, ColumnarValue, ExprSchemable, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
    Volatility,
};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::error::{Result, VegaFusionError};

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

    let timestamptz_expr =
        to_timestamptz_expr(&args[0], schema, &tz_config.default_input_tz.to_string())?;
    let udf_args = vec![timestamptz_expr, lit(format_str), lit(format_tz_str)];

    Ok(Expr::ScalarUDF {
        fun: Arc::new((*FORMAT_TIMESTAMPTZ_UDF).clone()),
        args: udf_args,
    })
}

pub fn utc_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;
    let timestamptz_expr =
        to_timestamptz_expr(&args[0], schema, &tz_config.default_input_tz.to_string())?;
    let udf_args = vec![timestamptz_expr, lit(format_str), lit("UTC")];
    Ok(Expr::ScalarUDF {
        fun: Arc::new((*FORMAT_TIMESTAMPTZ_UDF).clone()),
        args: udf_args,
    })
}

fn to_timestamptz_expr(arg: &Expr, schema: &DFSchema, default_input_tz: &str) -> Result<Expr> {
    Ok(match arg.get_type(schema)? {
        DataType::Timestamp(_, _) => arg.clone(),
        DataType::Utf8 => Expr::ScalarUDF {
            fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![arg.clone(), lit(default_input_tz)],
        },
        dtype if is_numeric_datatype(&dtype) => Expr::ScalarUDF {
            fun: Arc::new((*EPOCH_MS_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![cast_to(arg.clone(), &DataType::Int64, schema)?, lit("UTC")],
        },
        dtype => {
            return Err(VegaFusionError::internal(format!(
                "Invalid argument type to timeFormat function: {:?}",
                dtype
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

    // Add compatibility adjustments from D3 to Chrono

    // %f is microseconds in D3 but nanoseconds, this is %6f is chrono
    let format_str = format_str.replace("%f", "%6f");

    // %L is milliseconds in D3, this is %f3f in chrono
    let format_str = format_str.replace("%L", "%3f");
    Ok(format_str)
}

pub fn make_time_format_udf() -> ScalarUDF {
    let time_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // Argument order
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        // [1] time format string
        let format_str = if let ColumnarValue::Scalar(format_str) = &args[1] {
            format_str.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected output_tz to be a scalar".to_string(),
            ));
        };

        // [2] output timezone string
        let output_tz = if let ColumnarValue::Scalar(output_tz) = &args[2] {
            output_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected output_tz to be a scalar".to_string(),
            ));
        };
        let output_tz = chrono_tz::Tz::from_str(&output_tz).map_err(|_err| {
            DataFusionError::Internal(format!("Failed to parse {} as a timezone", output_tz))
        })?;

        let utc_millis_array = data_array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let formatted = Arc::new(StringArray::from_iter(utc_millis_array.iter().map(
            |utc_millis| {
                utc_millis.map(|utc_millis| {
                    // Load as UTC datetime
                    let utc_seconds = utc_millis / 1_000;
                    let utc_nanos = (utc_millis % 1_000 * 1_000_000) as u32;
                    let naive_utc_datetime = NaiveDateTime::from_timestamp(utc_seconds, utc_nanos);

                    // Convert to local timezone
                    let datetime: DateTime<chrono_tz::Tz> =
                        output_tz.from_utc_datetime(&naive_utc_datetime);

                    // Format as string
                    let formatted = datetime.format(&format_str);
                    formatted.to_string()
                })
            },
        ))) as ArrayRef;

        // maybe back to scalar
        if formatted.len() != 1 {
            Ok(ColumnarValue::Array(formatted))
        } else {
            ScalarValue::try_from_array(&formatted, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    let signature: Signature = Signature::exact(
        vec![
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Utf8,
            DataType::Utf8,
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("format_timestamptz", &signature, &return_type, &time_fn)
}

lazy_static! {
    pub static ref FORMAT_TIMESTAMPTZ_UDF: ScalarUDF = make_time_format_udf();
}
