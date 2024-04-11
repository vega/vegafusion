use chrono::{NaiveDateTime, TimeZone};
use std::any::Any;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::compute::try_unary;
use vegafusion_common::arrow::error::ArrowError;
use vegafusion_common::datafusion_expr::ScalarUDFImpl;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, Date32Array, TimestampMillisecondArray},
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility},
};

#[derive(Debug, Clone)]
pub struct DateToUtcTimestampUDF {
    signature: Signature,
}

impl Default for DateToUtcTimestampUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl DateToUtcTimestampUDF {
    pub fn new() -> Self {
        let signature = Signature::exact(
            vec![DataType::Date32, DataType::Utf8],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for DateToUtcTimestampUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_to_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // [0] data array
        let date_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
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

        let timestamp_array: TimestampMillisecondArray = try_unary(date_array, |v| {
            // Build naive datetime for time
            let seconds = (v as i64) * s_per_day;
            let nanoseconds = 0_u32;
            let naive_local_datetime = NaiveDateTime::from_timestamp_opt(seconds, nanoseconds)
                .expect("invalid or out-of-range datetime");

            // Compute UTC date time when naive date time is interpreted in the provided timezone
            let local_datetime = tz
                .from_local_datetime(&naive_local_datetime)
                .earliest()
                .ok_or(ArrowError::ComputeError("date out of bounds".to_string()))?;

            // Get timestamp millis (in UTC)
            Ok(local_datetime.timestamp_millis())
        })?;
        let timestamp_array = Arc::new(timestamp_array) as ArrayRef;

        // maybe back to scalar
        if timestamp_array.len() != 1 {
            Ok(ColumnarValue::Array(timestamp_array))
        } else {
            ScalarValue::try_from_array(&timestamp_array, 0).map(ColumnarValue::Scalar)
        }
    }
}

lazy_static! {
    pub static ref DATE_TO_UTC_TIMESTAMP_UDF: ScalarUDF =
        ScalarUDF::from(DateToUtcTimestampUDF::new());
}
