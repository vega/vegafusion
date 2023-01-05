/*!
## Date-Time Functions
Functions for working with date-time values.

See: https://vega.github.io/vega/docs/expressions/#datetime-functions
*/
pub mod date_format;
pub mod date_parts;
pub mod date_to_timestamptz;
pub mod datetime;
pub mod epoch_to_timestamptz;
pub mod str_to_timestamptz;
pub mod time;
pub mod timestamp_to_timestamptz;
pub mod timestamptz_to_timestamp;

use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::datetime_strs_to_timestamp_millis;
use datafusion::arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use datafusion::arrow::compute::{cast, unary};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

pub fn process_input_datetime(arg: &ArrayRef, default_input_tz: &chrono_tz::Tz) -> ArrayRef {
    match arg.data_type() {
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
        _ => panic!("Unexpected data type for date part function:"),
    }
}
