use crate::udfs::datetime::from_utc_timestamp::from_utc_timestamp;
use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;
use chrono::NaiveDateTime;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::TypeSignature;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_utc_timestamp_to_str_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // Argument order
        // [0] data array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        let timestamp_array = to_timestamp_ms(&timestamp_array)?;
        if matches!(timestamp_array.data_type(), DataType::Null) {
            return Ok(ColumnarValue::Array(timestamp_array));
        }

        // [1] timezone string
        let tz_str = if let ColumnarValue::Scalar(default_input_tz) = &args[1] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected default_input_tz to be a scalar".to_string(),
            ));
        };

        // Convert timestamp to desired time zone
        let timestamp_in_tz = if tz_str == "UTC" {
            timestamp_array
        } else {
            let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
                DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
            })?;
            from_utc_timestamp(timestamp_array, tz)?
        };

        let timestamp_in_tz = timestamp_in_tz
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let formatted = Arc::new(StringArray::from_iter(timestamp_in_tz.iter().map(
            |utc_millis| {
                utc_millis.and_then(|utc_millis| {
                    // Load as UTC datetime
                    let utc_seconds = utc_millis / 1_000;
                    let utc_nanos = (utc_millis % 1_000 * 1_000_000) as u32;
                    NaiveDateTime::from_timestamp_opt(utc_seconds, utc_nanos).map(
                        |naive_datetime| {
                            let formatted = naive_datetime.format("%Y-%m-%dT%H:%M:%S.%3f");
                            formatted.to_string()
                        },
                    )
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

    let signature = Signature::one_of(
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

    ScalarUDF::new("utc_timestamp_to_str", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref UTC_TIMESTAMP_TO_STR_UDF: ScalarUDF = make_utc_timestamp_to_str_udf();
}
