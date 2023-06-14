use chrono::TimeZone;
use chrono::{NaiveDateTime, Timelike};
use chrono_tz::Tz;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::array::Array;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, TimestampMillisecondArray},
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_to_utc_timestamp_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        // [1] timezone string
        let tz_str = if let ColumnarValue::Scalar(default_input_tz) = &args[1] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected default_input_tz to be a scalar".to_string(),
            ));
        };
        let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
            DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
        })?;

        let result_array = to_utc_timestamp(timestamp_array, tz)?;

        // maybe back to scalar
        if result_array.len() != 1 {
            Ok(ColumnarValue::Array(result_array))
        } else {
            ScalarValue::try_from_array(&result_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    // Signature should be (Timestamp, UTF8), but specifying Timestamp in the signature
    // requires specifying the timezone explicitly, and DataFusion doesn't currently
    // coerce between timezones.
    let signature: Signature = Signature::any(2, Volatility::Immutable);

    ScalarUDF::new("to_utc_timestamp", &signature, &return_type, &scalar_fn)
}

pub fn to_utc_timestamp(timestamp_array: ArrayRef, tz: Tz) -> Result<ArrayRef, DataFusionError> {
    // Normalize input to integer array of milliseconds
    let timestamp_array = to_timestamp_ms(&timestamp_array)?;
    let timestamp_millis = timestamp_array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let timestamp_array = TimestampMillisecondArray::from(
        timestamp_millis
            .iter()
            .map(|v| {
                v.and_then(|v| {
                    // Build naive datetime for time
                    let seconds = v / 1000;
                    let milliseconds = v % 1000;
                    let nanoseconds = (milliseconds * 1_000_000) as u32;
                    let naive_local_datetime =
                        NaiveDateTime::from_timestamp_opt(seconds, nanoseconds)?;

                    // Get UTC offset when the naive datetime is considered to be in local time
                    let local_datetime = if let Some(local_datetime) =
                        tz.from_local_datetime(&naive_local_datetime).earliest()
                    {
                        Some(local_datetime)
                    } else {
                        // Try adding 1 hour to handle daylight savings boundaries
                        let hour = naive_local_datetime.hour();
                        let new_naive_local_datetime =
                            naive_local_datetime.with_hour(hour + 1).unwrap();
                        tz.from_local_datetime(&new_naive_local_datetime).earliest()
                    };

                    // Get timestamp millis (in UTC)
                    local_datetime.map(|local_datetime| local_datetime.timestamp_millis())
                })
            })
            .collect::<Vec<Option<_>>>(),
    );

    Ok(Arc::new(timestamp_array) as ArrayRef)
}

pub fn to_timestamp_ms(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::Timestamp(time_unit, _) => {
            if time_unit == &TimeUnit::Millisecond {
                Ok(array.clone())
            } else {
                Ok(cast(
                    array,
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                )?)
            }
        }
        DataType::Date32 => Ok(cast(
            array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?),
        DataType::Date64 => Ok(cast(
            array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?),
        dtype => Err(DataFusionError::Internal(format!(
            "Unexpected datatime in to_timestamp_ms: {dtype:?}"
        ))),
    }
}

lazy_static! {
    pub static ref TO_UTC_TIMESTAMP_UDF: ScalarUDF = make_to_utc_timestamp_udf();
}
