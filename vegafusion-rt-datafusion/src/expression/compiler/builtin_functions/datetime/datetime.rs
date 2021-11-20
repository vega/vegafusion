use crate::expression::compiler::builtin_functions::datetime::date_parsing::DATETIME_TO_MILLIS_LOCAL;
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype, is_string_datatype};
use chrono::{DateTime, Local, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::functions::{
    BuiltinScalarFunction, ReturnTypeFunction, ScalarFunctionImplementation, Signature,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::ops::Deref;
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};

pub fn datetime_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        // Datetime from string or integer in milliseconds
        let mut arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

        if let DataType::Timestamp(_unit, _) = dtype {
            // Single input is already a timestamp. Just convert to Milliseconds and return
            return cast_to(
                arg,
                &DataType::Timestamp(TimeUnit::Millisecond, None),
                schema,
            );
        }

        // Cast single numeric arg to Int64 for compatibility with ToTimestampMillis
        if is_numeric_datatype(&dtype) && !matches!(&dtype, &DataType::Int64) {
            arg = cast_to(arg, &DataType::Int64, schema)?
        }

        if is_string_datatype(&dtype) {
            arg = Expr::ScalarUDF {
                fun: Arc::new(DATETIME_TO_MILLIS_LOCAL.deref().clone()),
                args: vec![arg],
            }
        }

        Ok(Expr::ScalarFunction {
            fun: BuiltinScalarFunction::ToTimestampMillis,
            args: vec![arg],
        })
    } else {
        // Numeric date components
        let int_args: Vec<_> = args
            .iter()
            .map(|arg| cast_to(arg.clone(), &DataType::Int64, schema))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::ScalarUDF {
            fun: Arc::new(DATETIME_COMPONENTS.deref().clone()),
            args: int_args,
        })
    }
}

lazy_static! {
    pub static ref DATETIME_COMPONENTS: ScalarUDF = make_datetime_components_udf(false);
    pub static ref UTC_COMPONENTS: ScalarUDF = make_datetime_components_udf(true);
}

pub fn make_datetime_components_udf(utc: bool) -> ScalarUDF {
    let datetime_components: ScalarFunctionImplementation =
        Arc::new(move |args: &[ColumnarValue]| {
            // pad with defaults out to 7 arguments
            let num_args = args.len();
            let mut args = Vec::from(args);

            if args.len() < 3 {
                // default to 1st of the month
                args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
            }

            // Remaining args (hour, minute, second, millisecond) default to zero
            for _ in num_args..7 {
                args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(0))));
            }

            // first, identify if any of the arguments is an Array. If yes, store its `len`,
            // as any scalar will need to be converted to an array of len `len`.
            let len = args
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
            let years = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
            let months = args[1].as_any().downcast_ref::<Int64Array>().unwrap();
            let days = args[2].as_any().downcast_ref::<Int64Array>().unwrap();
            let hours = args[3].as_any().downcast_ref::<Int64Array>().unwrap();
            let minutes = args[4].as_any().downcast_ref::<Int64Array>().unwrap();
            let seconds = args[5].as_any().downcast_ref::<Int64Array>().unwrap();
            let milliseconds = args[6].as_any().downcast_ref::<Int64Array>().unwrap();

            let num_rows = years.len();
            let mut datetime_builder = TimestampMillisecondArray::builder(num_rows);

            for i in 0..num_rows {
                if years.is_null(i)
                    || months.is_null(i)
                    || days.is_null(i)
                    || hours.is_null(i)
                    || minutes.is_null(i)
                    || seconds.is_null(i)
                    || milliseconds.is_null(i)
                {
                    // If any component is null, propagate null
                    datetime_builder.append_null().unwrap();
                } else {
                    let year = years.value(i);
                    let month = months.value(i);
                    let day = days.value(i);
                    let hour = hours.value(i);
                    let minute = minutes.value(i);
                    let second = seconds.value(i);
                    let millisecond = milliseconds.value(i);

                    // Treat 00-99 as 1900 to 1999
                    let mut year = year;
                    if (0..100).contains(&year) {
                        year += 1900
                    }

                    let timestamp = if utc {
                        let datetime: Option<DateTime<_>> = Utc
                            .ymd_opt(year as i32, month as u32 + 1, day as u32)
                            .single()
                            .and_then(|date| {
                                date.and_hms_milli_opt(
                                    hour as u32,
                                    minute as u32,
                                    second as u32,
                                    millisecond as u32,
                                )
                            });
                        if let Some(datetime) = datetime {
                            datetime.timestamp_millis()
                        } else {
                            // Invalid date
                            datetime_builder.append_null().unwrap();
                            continue;
                        }
                    } else {
                        let naive_date =
                            NaiveDate::from_ymd(year as i32, month as u32 + 1, day as u32);
                        let naive_time = NaiveTime::from_hms_milli(
                            hour as u32,
                            minute as u32,
                            second as u32,
                            millisecond as u32,
                        );
                        let naive_datetime = NaiveDateTime::new(naive_date, naive_time);
                        naive_datetime_to_timestamp(naive_datetime)?
                    };

                    datetime_builder.append_value(timestamp).unwrap();
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

    // vega signature: datetime(year, month[, day, hour, min, sec, millisec])
    let sig = |n: usize| vec![DataType::Int64; n];
    let signature = Signature::one_of(
        vec![
            TypeSignature::Exact(sig(2)),
            TypeSignature::Exact(sig(3)),
            TypeSignature::Exact(sig(4)),
            TypeSignature::Exact(sig(5)),
            TypeSignature::Exact(sig(6)),
            TypeSignature::Exact(sig(7)),
        ],
        Volatility::Immutable,
    );
    ScalarUDF::new(
        "datetime_components",
        &signature,
        &return_type,
        &datetime_components,
    )
}

/// Helper from DataFusion:
/// Converts the naive datetime (which has no specific timezone) to a
/// millisecond epoch timestamp relative to UTC.
fn naive_datetime_to_timestamp(
    datetime: NaiveDateTime,
) -> std::result::Result<i64, DataFusionError> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => Err(DataFusionError::Internal(format!(
            "Failed to convert datetime to local timezone: {}",
            datetime
        ))),
        LocalResult::Single(local_datetime) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_millis())
        }
        // Ambiguous times can happen if the timestamp is exactly when
        // a daylight savings time transition occurs, for example, and
        // so the datetime could validly be said to be in two
        // potential offsets. However, since we are about to convert
        // to UTC anyways, we can pick one arbitrarily
        LocalResult::Ambiguous(local_datetime, _) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_millis())
        }
    }
}
