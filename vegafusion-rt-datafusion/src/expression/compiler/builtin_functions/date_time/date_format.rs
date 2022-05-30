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
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::str::FromStr;
use std::sync::Arc;
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

    Ok(Expr::ScalarUDF {
        fun: Arc::new(make_time_format_udf(
            &tz_config.default_input_tz,
            &output_tz,
            &format_str,
        )),
        args: Vec::from(&args[..1]),
    })
}

pub fn utc_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    _schema: &DFSchema,
) -> Result<Expr> {
    let format_str = extract_format_str(args)?;
    Ok(Expr::ScalarUDF {
        fun: Arc::new(make_time_format_udf(
            &tz_config.default_input_tz,
            &chrono_tz::UTC,
            &format_str,
        )),
        args: Vec::from(&args[..1]),
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

pub fn make_time_format_udf(
    default_input_tz: &chrono_tz::Tz,
    output_tz: &chrono_tz::Tz,
    format_str: &str,
) -> ScalarUDF {
    let default_input_tz = *default_input_tz;
    let output_tz = *output_tz;
    let format_str = format_str.to_string();
    let time_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        let arg = process_input_datetime(arg, &default_input_tz);

        let utc_millis_array = arg.as_any().downcast_ref::<Int64Array>().unwrap();

        let formatted = StringArray::from_iter(utc_millis_array.iter().map(|utc_millis| {
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
        }));

        Ok(Arc::new(formatted) as ArrayRef)
    };
    let time_fn = make_scalar_function(time_fn);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "timeFormat",
        &Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Date32,
                DataType::Date64,
                DataType::Int64,
                DataType::Float64,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &time_fn,
    )
}
