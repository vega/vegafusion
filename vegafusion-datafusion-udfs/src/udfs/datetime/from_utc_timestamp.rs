use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Tz;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::array::Array;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, TimestampMillisecondArray},
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        TypeSignature, Volatility,
    },
};

use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;

fn make_from_utc_timestamp() -> ScalarUDF {
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

        let result_array = from_utc_timestamp(timestamp_array, tz)?;

        // maybe back to scalar
        if result_array.len() != 1 {
            Ok(ColumnarValue::Array(result_array))
        } else {
            ScalarValue::try_from_array(&result_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature: Signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Date64, DataType::Utf8]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
            ]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Utf8,
            ]),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("from_utc_timestamp", &signature, &return_type, &scalar_fn)
}

pub fn from_utc_timestamp(timestamp_array: ArrayRef, tz: Tz) -> Result<ArrayRef, DataFusionError> {
    let timestamp_array = to_timestamp_ms(&timestamp_array)?;
    let timestamp_array = timestamp_array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();

    let timestamp_array = TimestampMillisecondArray::from(
        timestamp_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    // Build naive datetime for time
                    let seconds = v / 1000;
                    let milliseconds = v % 1000;
                    let nanoseconds = (milliseconds * 1_000_000) as u32;
                    let naive_utc_datetime =
                        NaiveDateTime::from_timestamp_opt(seconds, nanoseconds)
                            .expect("invalid or out-of-range datetime");

                    // Create local datetime, interpreting the naive datetime as utc
                    let local_datetime = tz.from_utc_datetime(&naive_utc_datetime);
                    let naive_local_datetime = local_datetime.naive_local();

                    naive_local_datetime.timestamp_millis()
                })
            })
            .collect::<Vec<Option<_>>>(),
    );

    Ok(Arc::new(timestamp_array) as ArrayRef)
}

lazy_static! {
    pub static ref FROM_UTC_TIMESTAMP_UDF: ScalarUDF = make_from_utc_timestamp();
}
