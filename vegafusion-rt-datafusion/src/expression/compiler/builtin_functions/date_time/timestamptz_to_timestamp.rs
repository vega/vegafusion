use crate::expression::compiler::builtin_functions::date_time::timestamp_to_timestamptz::to_timestamp_ms;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use datafusion::common::DataFusionError;
use datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::arrow::array::{ArrayRef, TimestampMillisecondArray};
use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_core::data::scalar::ScalarValue;

pub fn make_timestamptz_to_timestamp() -> ScalarUDF {
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
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "timestamptz_to_timestamp",
        &signature,
        &return_type,
        &scalar_fn,
    )
}

lazy_static! {
    pub static ref TIMESTAMPTZ_TO_TIMESTAMP_UDF: ScalarUDF = make_timestamptz_to_timestamp();
}
