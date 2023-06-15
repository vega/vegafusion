pub mod date_add_tz;
pub mod date_part_tz;
pub mod date_to_utc_timestamp;
pub mod date_trunc_tz;
pub mod epoch_to_utc_timestamp;
pub mod format_timestamp;
pub mod from_utc_timestamp;
pub mod make_utc_timestamp;
pub mod str_to_utc_timestamp;
pub mod timeunit;
pub mod to_utc_timestamp;
pub mod utc_timestamp_to_epoch;
pub mod utc_timestamp_to_str;

use crate::udfs::datetime::str_to_utc_timestamp::datetime_strs_to_timestamp_millis;
use std::sync::Arc;
use vegafusion_common::arrow::{
    array::{ArrayRef, Date32Array, Int64Array, StringArray},
    compute::{cast, unary},
    datatypes::{DataType, TimeUnit},
};
use vegafusion_common::datafusion_common::DataFusionError;

pub fn process_input_datetime(
    arg: &ArrayRef,
    default_input_tz: &chrono_tz::Tz,
) -> Result<ArrayRef, DataFusionError> {
    Ok(match arg.data_type() {
        DataType::Utf8 => {
            let array = arg.as_any().downcast_ref::<StringArray>().unwrap();
            cast(
                &datetime_strs_to_timestamp_millis(array, &Some(*default_input_tz)),
                &DataType::Int64,
            )
            .expect("Failed to case timestamp to Int64")
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            cast(arg, &DataType::Int64).expect("Failed to case timestamp to Int64")
        }
        DataType::Timestamp(_, _) => {
            let arg_ms = cast(arg, &DataType::Timestamp(TimeUnit::Millisecond, None))
                .expect("Failed to convert timestamp[ns] to timestamp[ms]");
            cast(&arg_ms, &DataType::Int64).expect("Failed to case timestamp to Int64")
        }
        DataType::Date32 => {
            let ms_per_day = 1000 * 60 * 60 * 24_i64;
            let array = arg.as_any().downcast_ref::<Date32Array>().unwrap();

            let array: Int64Array = unary(array, |v| (v as i64) * ms_per_day);
            Arc::new(array) as ArrayRef as _
        }
        DataType::Date64 => {
            let int_array = cast(arg, &DataType::Int64).unwrap();
            int_array
        }
        DataType::Int64 => arg.clone(),
        DataType::Float64 => cast(arg, &DataType::Int64).expect("Failed to cast float to int"),
        _ => {
            return Err(DataFusionError::Internal(
                "Unexpected data type for date part function:".to_string(),
            ))
        }
    })
}
