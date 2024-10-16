use chrono::{
    format::{parse, Parsed, StrftimeItems},
    {DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike, Utc},
};
use regex::Regex;
use std::any::Any;
use std::{str::FromStr, sync::Arc};
use vegafusion_common::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use vegafusion_core::planning::parse_datetime::parse_datetime;

pub fn parse_datetime_to_utc_millis(
    date_str: &str,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> Option<i64> {
    // Parse to datetime
    let parsed_utc = parse_datetime(date_str, default_input_tz)?;

    // Extract milliseconds
    Some(parsed_utc.timestamp_millis())
}

pub fn datetime_strs_to_timestamp_millis(
    date_strs: &StringArray,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> ArrayRef {
    let millis_array = TimestampMillisecondArray::from(
        date_strs
            .iter()
            .map(|date_str| -> Option<i64> {
                date_str
                    .and_then(|date_str| parse_datetime_to_utc_millis(date_str, default_input_tz))
            })
            .collect::<Vec<Option<i64>>>(),
    );

    Arc::new(millis_array) as ArrayRef
}

#[derive(Debug, Clone)]
pub struct StrToUtcTimestampUDF {
    signature: Signature,
}

impl Default for StrToUtcTimestampUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl StrToUtcTimestampUDF {
    pub fn new() -> Self {
        let signature =
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for StrToUtcTimestampUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "str_to_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> vegafusion_common::datafusion_common::Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        // [0] data array
        let str_array = match &args[0] {
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

        let str_array = str_array.as_any().downcast_ref::<StringArray>().unwrap();

        let timestamp_array = datetime_strs_to_timestamp_millis(str_array, &Some(tz));

        // maybe back to scalar
        if timestamp_array.len() != 1 {
            Ok(ColumnarValue::Array(timestamp_array))
        } else {
            ScalarValue::try_from_array(&timestamp_array, 0).map(ColumnarValue::Scalar)
        }
    }
}

lazy_static! {
    pub static ref STR_TO_UTC_TIMESTAMP_UDF: ScalarUDF =
        ScalarUDF::from(StrToUtcTimestampUDF::new());
}

#[test]
fn test_parse_datetime() {
    let local_tz = Some(chrono_tz::Tz::America__New_York);
    let utc = Some(chrono_tz::Tz::UTC);
    let res = parse_datetime("2020-05-16T09:30:00+05:00", &utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");

    let res = parse_datetime("2020-05-16T09:30:00", &utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");

    let res = parse_datetime("2020-05-16T09:30:00", &local_tz).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");

    let res = parse_datetime("2001/02/05 06:20", &local_tz).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");

    let res = parse_datetime("2001/02/05 06:20", &utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");

    let res = parse_datetime("2000-01-01T08:00:00.000Z", &utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {res}");
    println!("utc_res: {utc_res}");
}
