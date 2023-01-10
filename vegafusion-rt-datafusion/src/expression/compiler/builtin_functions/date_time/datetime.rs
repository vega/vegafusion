use crate::expression::compiler::builtin_functions::date_time::epoch_to_timestamptz::EPOCH_MS_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype, is_string_datatype};
use crate::task_graph::timezone::RuntimeTzConfig;
use chrono::{DateTime, TimeZone, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{
    lit, ReturnTypeFunction, ScalarFunctionImplementation, Signature, Volatility,
};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::arrow::array::TimestampMillisecondBuilder;
use vegafusion_core::arrow::compute::cast;
use vegafusion_core::arrow::datatypes::TimeUnit;
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
        .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

    if is_string_datatype(&dtype) {
        let default_input_tz = if args.len() == 2 {
            // Second argument is a an override local timezone string
            let input_tz_expr = &args[1];
            if let Expr::Literal(ScalarValue::Utf8(Some(input_tz_str))) = input_tz_expr {
                if input_tz_str == "local" {
                    tz_config.local_tz
                } else {
                    chrono_tz::Tz::from_str(input_tz_str)
                        .ok()
                        .with_context(|| {
                            format!("Failed to parse {} as a timezone", input_tz_str)
                        })?
                }
            } else {
                return Err(VegaFusionError::parse(
                    "Second argument to toDate must be a timezone string",
                ));
            }
        } else {
            tz_config.default_input_tz
        };

        Ok(Expr::ScalarUDF {
            fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![arg, lit(default_input_tz.to_string())],
        })
    } else if is_numeric_datatype(&dtype) {
        Ok(Expr::ScalarUDF {
            fun: Arc::new((*EPOCH_MS_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![cast_to(arg, &DataType::Int64, schema)?, lit("UTC")],
        })
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
        let mut arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

        if is_string_datatype(&dtype) {
            let default_input_tz_str = tz_config.default_input_tz.to_string();
            arg = Expr::ScalarUDF {
                fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
                args: vec![arg, lit(default_input_tz_str)],
            }
        }

        cast_to(arg, &DataType::Int64, schema)
    } else {
        let udf_args =
            extract_datetime_component_args(args, &tz_config.default_input_tz.to_string(), schema)?;
        Ok(Expr::ScalarUDF {
            fun: Arc::new((*MAKE_TIMESTAMPTZ).clone()),
            args: udf_args,
        })
    }
}

pub fn make_datetime_components_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let udf_args =
        extract_datetime_component_args(args, &tz_config.default_input_tz.to_string(), schema)?;
    Ok(Expr::ScalarUDF {
        fun: Arc::new(MAKE_TIMESTAMPTZ.deref().clone()),
        args: udf_args,
    })
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

pub fn make_datetime_components_udf() -> ScalarUDF {
    let datetime_components: ScalarFunctionImplementation =
        Arc::new(move |args: &[ColumnarValue]| {
            let tz_str = if let ColumnarValue::Scalar(tz_scalar) = &args[7] {
                tz_scalar.to_string()
            } else {
                return Err(DataFusionError::Internal(
                    "Expected timezone to be a scalar".to_string(),
                ));
            };

            let input_tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
                DataFusionError::Internal(format!("Failed to parse {} as a timezone", tz_str))
            })?;

            // first, identify if any of the arguments is an Array. If yes, store its `len`,
            // as any scalar will need to be converted to an array of len `len`.
            let len = args[..7]
                .iter()
                .fold(Option::<usize>::None, |acc, arg| match arg {
                    ColumnarValue::Scalar(_) => acc,
                    ColumnarValue::Array(a) => Some(a.len()),
                });

            // to arrays
            let args = if let Some(len) = len {
                args.iter()
                    .map(|arg| arg.clone().into_array(len))
                    .collect::<Vec<ArrayRef>>()
            } else {
                args.iter()
                    .map(|arg| arg.clone().into_array(1))
                    .collect::<Vec<ArrayRef>>()
            };

            // To int64 arrays
            let years = cast(&args[0], &DataType::Int64).unwrap();
            let years = years.as_any().downcast_ref::<Int64Array>().unwrap();

            let months = cast(&args[1], &DataType::Int64).unwrap();
            let months = months.as_any().downcast_ref::<Int64Array>().unwrap();

            let days = cast(&args[2], &DataType::Int64).unwrap();
            let days = days.as_any().downcast_ref::<Int64Array>().unwrap();

            let hours = cast(&args[3], &DataType::Int64).unwrap();
            let hours = hours.as_any().downcast_ref::<Int64Array>().unwrap();

            let minutes = cast(&args[4], &DataType::Int64).unwrap();
            let minutes = minutes.as_any().downcast_ref::<Int64Array>().unwrap();

            let seconds = cast(&args[5], &DataType::Int64).unwrap();
            let seconds = seconds.as_any().downcast_ref::<Int64Array>().unwrap();

            let millis = cast(&args[6], &DataType::Int64).unwrap();
            let millis = millis.as_any().downcast_ref::<Int64Array>().unwrap();

            let num_rows = years.len();
            let mut datetime_builder = TimestampMillisecondBuilder::new();

            for i in 0..num_rows {
                if years.is_null(i)
                    || months.is_null(i)
                    || days.is_null(i)
                    || hours.is_null(i)
                    || minutes.is_null(i)
                    || seconds.is_null(i)
                    || millis.is_null(i)
                {
                    // If any component is null, propagate null
                    datetime_builder.append_null();
                } else {
                    let year = years.value(i);
                    let month = months.value(i);
                    let day = days.value(i);
                    let hour = hours.value(i);
                    let minute = minutes.value(i);
                    let second = seconds.value(i);
                    let milli = millis.value(i);

                    // Treat 00-99 as 1900 to 1999
                    let mut year = year;
                    if (0..100).contains(&year) {
                        year += 1900
                    }

                    let datetime: Option<DateTime<_>> = input_tz
                        .with_ymd_and_hms(
                            year as i32,
                            month as u32 + 1,
                            day as u32,
                            hour as u32,
                            minute as u32,
                            second as u32,
                        )
                        .single()
                        .and_then(|date: DateTime<_>| {
                            date.with_nanosecond((milli * 1_000_000) as u32)
                        });

                    if let Some(datetime) = datetime {
                        // Always convert to UTC
                        let datetime = datetime.with_timezone(&chrono::Utc);
                        datetime_builder.append_value(datetime.timestamp_millis());
                    } else {
                        // Invalid date
                        datetime_builder.append_null();
                    }
                }
            }

            let result = Arc::new(datetime_builder.finish()) as ArrayRef;

            // maybe back to scalar
            if len.is_some() {
                Ok(ColumnarValue::Array(result))
            } else {
                ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
            }
        });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature = Signature::exact(
        vec![
            DataType::Float64, // year
            DataType::Float64, // month
            DataType::Float64, // date
            DataType::Float64, // hour
            DataType::Float64, // minute
            DataType::Float64, // second
            DataType::Float64, // millisecond
            DataType::Utf8,    // time zone
        ],
        Volatility::Immutable,
    );
    ScalarUDF::new(
        "make_timestamptz",
        &signature,
        &return_type,
        &datetime_components,
    )
}

lazy_static! {
    pub static ref MAKE_TIMESTAMPTZ: ScalarUDF = make_datetime_components_udf();
}
