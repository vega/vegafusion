/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
/*!
## Date-Time Functions
Functions for working with date-time values.

See: https://vega.github.io/vega/docs/expressions/#datetime-functions
*/
pub mod date_format;
pub mod date_parsing;
pub mod date_parts;
pub mod datetime;
pub mod local_to_utc;
pub mod time;

use crate::expression::compiler::builtin_functions::date_time::date_parsing::{
    datetime_strs_to_millis, DateParseMode,
};
use datafusion::arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use datafusion::arrow::compute::{cast, unary};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

fn process_input_datetime(arg: &ArrayRef, default_input_tz: &chrono_tz::Tz) -> ArrayRef {
    match arg.data_type() {
        DataType::Utf8 => {
            let array = arg.as_any().downcast_ref::<StringArray>().unwrap();
            datetime_strs_to_millis(array, DateParseMode::JavaScript, &Some(*default_input_tz)) as _
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            cast(arg, &DataType::Int64).expect("Failed to case timestamp to Int64")
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
        DataType::Float64 => cast(arg, &DataType::Int64).expect("Failed to case float to int"),
        _ => panic!("Unexpected data type for date part function:"),
    }
}
