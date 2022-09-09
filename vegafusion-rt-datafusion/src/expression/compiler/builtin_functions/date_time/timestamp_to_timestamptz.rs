use chrono::TimeZone;
use chrono::{NaiveDateTime, Timelike};
use datafusion::arrow::array::Int64Array;
use datafusion::common::DataFusionError;
use datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::arrow::array::{ArrayRef, TimestampMillisecondArray};
use vegafusion_core::arrow::compute::{cast, unary};
use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_core::data::scalar::ScalarValue;

pub fn make_timestamp_to_timestamptz() -> ScalarUDF {
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
            DataFusionError::Internal(format!("Failed to parse {} as a timezone", tz_str))
        })?;

        // Normalize input to integer array of milliseconds
        let millis_array = match timestamp_array.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => {
                cast(&timestamp_array, &DataType::Int64)?
            }
            DataType::Timestamp(_, _) => {
                let timestamp_millis = cast(
                    &timestamp_array,
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                )?;
                cast(&timestamp_millis, &DataType::Int64)?
            }
            dtype => {
                return Err(DataFusionError::Internal(format!(
                    "Unexpected data type for timestamp_to_timestamptz: {:?}",
                    dtype
                )))
            }
        };

        let millis_array = millis_array.as_any().downcast_ref::<Int64Array>().unwrap();
        let timestamp_array = millis_to_timestamp(millis_array, tz);
        let timestamp_array = Arc::new(timestamp_array) as ArrayRef;

        // maybe back to scalar
        if timestamp_array.len() != 1 {
            Ok(ColumnarValue::Array(timestamp_array))
        } else {
            ScalarValue::try_from_array(&timestamp_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature: Signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
            ]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Utf8,
            ]),
            TypeSignature::Exact(vec![DataType::Date64, DataType::Utf8]),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "timestamp_to_timestamptz",
        &signature,
        &return_type,
        &scalar_fn,
    )
}

pub fn millis_to_timestamp(
    millis_array: &Int64Array,
    tz: chrono_tz::Tz,
) -> TimestampMillisecondArray {
    unary(millis_array, |v| {
        // Build naive datetime for time
        let seconds = v / 1000;
        let milliseconds = v % 1000;
        let nanoseconds = (milliseconds * 1_000_000) as u32;
        let naive_local_datetime = NaiveDateTime::from_timestamp(seconds, nanoseconds);

        // Get UTC offset when the naive datetime is considered to be in local time
        let local_datetime = if let Some(local_datetime) =
            tz.from_local_datetime(&naive_local_datetime).earliest()
        {
            local_datetime
        } else {
            // Try adding 1 hour to handle daylight savings boundaries
            let hour = naive_local_datetime.hour();
            let new_naive_local_datetime = naive_local_datetime.with_hour(hour + 1).unwrap();
            tz.from_local_datetime(&new_naive_local_datetime)
                .earliest()
                .unwrap_or_else(|| panic!("Failed to convert {:?}", naive_local_datetime))
        };

        // Get timestamp millis (in UTC)
        local_datetime.timestamp_millis()
    })
}

lazy_static! {
    pub static ref TIMESTAMP_TO_TIMESTAMPTZ_UDF: ScalarUDF = make_timestamp_to_timestamptz();
}
