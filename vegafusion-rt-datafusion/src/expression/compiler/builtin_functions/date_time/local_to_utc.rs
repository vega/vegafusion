/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */

use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::task_graph::timezone::RuntimeTzConfig;
use chrono::{NaiveDateTime, TimeZone, Timelike};
use datafusion::arrow::array::Int64Array;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;
use vegafusion_core::arrow::array::ArrayRef;
use vegafusion_core::arrow::compute::unary;
use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};

pub fn make_to_utc_millis_fn(tz_config: &RuntimeTzConfig) -> ScalarUDF {
    let default_input_tz = tz_config.default_input_tz;
    let local_tz = tz_config.local_tz;

    let to_utc_millis_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single string argument
        let arg = &args[0];

        // Determine which timezone to interpret timestamps in based on datatype
        let input_tz = if matches!(arg.data_type(), &DataType::Date32) {
            local_tz
        } else {
            default_input_tz
        };

        let arg = process_input_datetime(arg, &input_tz);

        let naive_datetime_millis = arg.as_any().downcast_ref::<Int64Array>().unwrap();

        let array: Int64Array = unary(naive_datetime_millis, |v| {
            // Build naive datetime for time
            let seconds = v / 1000;
            let milliseconds = v % 1000;
            let nanoseconds = (milliseconds * 1_000_000) as u32;
            let naive_local_datetime = NaiveDateTime::from_timestamp(seconds, nanoseconds);

            // Get UTC offset when the naive datetime is considered to be in local time
            let local_datetime = if let Some(local_datetime) = input_tz
                .from_local_datetime(&naive_local_datetime)
                .earliest()
            {
                local_datetime
            } else {
                // Try adding 1 hour to handle daylight savings boundaries
                let hour = naive_local_datetime.hour();
                let new_naive_local_datetime = naive_local_datetime.with_hour(hour + 1).unwrap();
                input_tz
                    .from_local_datetime(&new_naive_local_datetime)
                    .earliest()
                    .unwrap_or_else(|| panic!("Failed to convert {:?}", naive_local_datetime))
            };

            // Get timestamp millis (in UTC)
            local_datetime.timestamp_millis()
        });
        Ok(Arc::new(array) as ArrayRef)
    };

    let to_utc_millis_fn = make_scalar_function(to_utc_millis_fn);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "to_utc_millis_fn",
        &Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Date32,
                DataType::Date64,
                DataType::Int64,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &to_utc_millis_fn,
    )
}
