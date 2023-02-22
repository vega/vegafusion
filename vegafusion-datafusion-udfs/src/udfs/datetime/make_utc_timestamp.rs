use chrono::{DateTime, TimeZone, Timelike};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::{
    arrow::{
        array::{Array, ArrayRef, Int64Array, TimestampMillisecondBuilder},
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_utc_timestamp_udf() -> ScalarUDF {
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
                DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
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
        "make_utc_timestamp",
        &signature,
        &return_type,
        &datetime_components,
    )
}

lazy_static! {
    pub static ref MAKE_UTC_TIMESTAMP: ScalarUDF = make_utc_timestamp_udf();
}
