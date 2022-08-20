/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::task_graph::timezone::RuntimeTzConfig;
use chrono::TimeZone;
use chrono::{DateTime, NaiveDateTime};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{ColumnarValue, lit, ReturnTypeFunction, ScalarFunctionImplementation, Signature, TypeSignature, Volatility};
use std::str::FromStr;
use std::sync::Arc;
use datafusion::common::DataFusionError;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

pub fn time_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    _schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;

    // Handle format timezone override
    let output_tz = if args.len() >= 3 {
        // Second argument is a an override local timezone string
        let format_tz_expr = &args[2];
        if let Expr::Literal(ScalarValue::Utf8(Some(format_tz_str))) = format_tz_expr {
            chrono_tz::Tz::from_str(format_tz_str)
                .ok()
                .with_context(|| format!("Failed to parse {} as a timezone", format_tz_str))?
        } else {
            return Err(VegaFusionError::parse(
                "Third argument to timeFormat must be a timezone string",
            ));
        }
    } else {
        tz_config.local_tz
    };

    let mut udf_args = vec![lit(tz_config.default_input_tz.to_string()), lit(output_tz.to_string())];
    udf_args.extend(Vec::from(&args[..1]));
    udf_args.push(lit(format_str));

    Ok(Expr::ScalarUDF {
        fun: Arc::new((*TIMEFORMAT_UDF).clone()),
        args: udf_args,
    })
}

pub fn utc_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    _schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;

    let mut udf_args = vec![lit(tz_config.default_input_tz.to_string()), lit("UTC")];
    udf_args.extend(Vec::from(&args[..1]));
    udf_args.push(lit(format_str));

    Ok(Expr::ScalarUDF {
        fun: Arc::new((*TIMEFORMAT_UDF).clone()),
        args: udf_args,
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
        // [0] default input timezone string
        let default_input_tz = if let ColumnarValue::Scalar(default_input_tz) = &args[0] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal("Expected default_input_tz to be a scalar".to_string()))
        };
        let default_input_tz = chrono_tz::Tz::from_str(&default_input_tz)
            .map_err(|err| DataFusionError::Internal(format!("Failed to parse {} as a timezone", default_input_tz)))?;

        // [1] output timezone string
        let output_tz = if let ColumnarValue::Scalar(output_tz) = &args[1] {
            output_tz.to_string()
        } else {
            return Err(DataFusionError::Internal("Expected output_tz to be a scalar".to_string()))
        };
        let output_tz = chrono_tz::Tz::from_str(&output_tz)
            .map_err(|err| DataFusionError::Internal(format!("Failed to parse {} as a timezone", output_tz)))?;

        // [2] data array
        let data_array = match &args[2] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()
        };

        // [3] time format string
        let format_str = if let ColumnarValue::Scalar(format_str) = &args[3] {
            format_str.to_string()
        } else {
            return Err(DataFusionError::Internal("Expected output_tz to be a scalar".to_string()))
        };

        let data_array = process_input_datetime(&data_array, &default_input_tz);
        let utc_millis_array = data_array.as_any().downcast_ref::<Int64Array>().unwrap();

        let formatted = Arc::new(StringArray::from_iter(utc_millis_array.iter().map(|utc_millis| {
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
        }))) as ArrayRef;

        // maybe back to scalar
        if formatted.len() != 1 {
            Ok(ColumnarValue::Array(formatted))
        } else {
            ScalarValue::try_from_array(&formatted, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    let signature: Signature = Signature::one_of(vec![
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Timestamp(TimeUnit::Millisecond, None)]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Date32]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Date64]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Int64]),
        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Float64]),
    ], Volatility::Immutable);

    ScalarUDF::new(
        "vg_timeformat",
        &signature,
        &return_type,
        &time_fn,
    )
}

lazy_static! {
    pub static ref TIMEFORMAT_UDF: ScalarUDF = make_time_format_udf();
}