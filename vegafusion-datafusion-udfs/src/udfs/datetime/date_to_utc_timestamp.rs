use chrono::{NaiveDateTime, TimeZone};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, Date32Array, TimestampMillisecondArray},
        compute::unary,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_date_to_utc_timestamp() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let date_array = match &args[0] {
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

        let s_per_day = 60 * 60 * 24_i64;
        let date_array = date_array.as_any().downcast_ref::<Date32Array>().unwrap();

        let timestamp_array: TimestampMillisecondArray = unary(date_array, |v| {
            // Build naive datetime for time
            let seconds = (v as i64) * s_per_day;
            let nanoseconds = 0_u32;
            let naive_local_datetime = NaiveDateTime::from_timestamp_opt(seconds, nanoseconds)
                .expect("invalid or out-of-range datetime");

            // Compute UTC date time when naive date time is interpreted in the provided timezone
            let local_datetime = tz
                .from_local_datetime(&naive_local_datetime)
                .earliest()
                .unwrap();

            // Get timestamp millis (in UTC)
            local_datetime.timestamp_millis()
        });
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

    let signature: Signature = Signature::exact(
        vec![DataType::Date32, DataType::Utf8],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "date_to_utc_timestamp",
        &signature,
        &return_type,
        &scalar_fn,
    )
}

lazy_static! {
    pub static ref DATE_TO_UTC_TIMESTAMP_UDF: ScalarUDF = make_date_to_utc_timestamp();
}
