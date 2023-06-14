use crate::udfs::datetime::process_input_datetime;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, Int64Array, TimestampMillisecondArray};
use vegafusion_common::arrow::compute::unary;
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_common::arrow::temporal_conversions::date64_to_datetime;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    TypeSignature, Volatility,
};

fn extract_bool(value: &ColumnarValue) -> std::result::Result<bool, DataFusionError> {
    if let ColumnarValue::Scalar(scalar) = value {
        if let ScalarValue::Boolean(Some(value)) = scalar {
            Ok(*value)
        } else {
            Err(DataFusionError::Internal(
                "expected boolean value".to_string(),
            ))
        }
    } else {
        Err(DataFusionError::Internal("unexpected argument".to_string()))
    }
}

fn unpack_timeunit_udf_args(
    columns: &[ColumnarValue],
) -> std::result::Result<(ArrayRef, chrono_tz::Tz, Vec<bool>), DataFusionError> {
    let tz_str = if let ColumnarValue::Scalar(scalar) = &columns[1] {
        scalar.to_string()
    } else {
        return Err(DataFusionError::Internal("unexpected argument".to_string()));
    };

    let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
        DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
    })?;

    let timestamp = columns[0].clone().into_array(1);
    let timestamp = process_input_datetime(&timestamp, &tz)?;

    Ok((
        timestamp,
        tz,
        vec![
            extract_bool(&columns[2])?,
            extract_bool(&columns[3])?,
            extract_bool(&columns[4])?,
            extract_bool(&columns[5])?,
            extract_bool(&columns[6])?,
            extract_bool(&columns[7])?,
            extract_bool(&columns[8])?,
            extract_bool(&columns[9])?,
            extract_bool(&columns[10])?,
            extract_bool(&columns[11])?,
            extract_bool(&columns[12])?,
        ],
    ))
}

/// For timestamp specified in UTC, perform time unit in the provided timezone (either UTC or Local)
fn perform_timeunit_start_from_utc<T: TimeZone>(
    value: i64,
    units_mask: &[bool],
    in_tz: T,
) -> DateTime<T> {
    // Load and interpret date time as UTC
    let dt_value = date64_to_datetime(value)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let dt_value = Utc.from_local_datetime(&dt_value).earliest().unwrap();
    let mut dt_value = dt_value.with_timezone(&in_tz);

    // Handle time truncation
    if !units_mask[7] {
        // Clear hours first to avoid any of the other time truncations from landing on a daylight
        // savings boundary
        dt_value = dt_value.with_hour(0).unwrap();
    }

    if !units_mask[10] {
        // Milliseconds
        let new_ns = (((dt_value.nanosecond() as f64) / 1e6).floor() * 1e6) as u32;
        dt_value = dt_value.with_nanosecond(new_ns).unwrap();
    }

    if !units_mask[9] {
        // Seconds
        dt_value = dt_value.with_second(0).unwrap();
    }

    if !units_mask[8] {
        // Minutes
        dt_value = dt_value.with_minute(0).unwrap();
    }

    // Save off day of the year and weekday here, because these will change if the
    // year is changed
    let ordinal0 = dt_value.ordinal0();
    let weekday = dt_value.weekday();

    // Handle year truncation
    // (if we're not truncating to week number, this is handled separately below)
    if !units_mask[0] && !units_mask[4] {
        // Year
        dt_value = if let Some(v) = dt_value.with_year(2012) {
            v
        } else {
            // The above can fail if changing to 2012 lands on daylight savings
            // e.g. March 11th at 2am in 2015
            let hour = dt_value.hour();
            dt_value
                .with_hour(0)
                .unwrap()
                .with_year(2012)
                .unwrap()
                .with_hour(hour + 1)
                .unwrap()
        }
    }

    // Handle date (of the year) truncation.
    // For simplicity, only one of these is valid at the same time for now
    if units_mask[1] {
        // Quarter
        // Truncate to Quarter
        let new_month = ((dt_value.month0() as f64 / 3.0).floor() * 3.0) as u32;
        dt_value = dt_value
            .with_day0(0)
            .unwrap()
            .with_month0(new_month)
            .unwrap();
    } else if units_mask[2] {
        // Month and not Date
        // Truncate to first day of the month
        if !units_mask[3] {
            dt_value = dt_value.with_day0(0).unwrap();
        }
    } else if units_mask[3] {
        // Date and not Month
        // Normalize to January, keeping existing day of the month.
        // (January has 31 days, so this is safe)
        if !units_mask[2] {
            dt_value = dt_value.with_month0(0).unwrap();
        }
    } else if units_mask[4] {
        // Week
        // Step 1: Find the date of the first Sunday in the same calendar year as the date.
        // This may occur in isoweek 0, or in the final isoweek of the previous year

        let isoweek0_sunday = NaiveDate::from_isoywd_opt(dt_value.year(), 1, Weekday::Sun)
            .expect("invalid or out-of-range datetime");

        let isoweek0_sunday = NaiveDateTime::new(isoweek0_sunday, dt_value.time());
        let isoweek0_sunday = in_tz
            .from_local_datetime(&isoweek0_sunday)
            .earliest()
            .unwrap();

        // Subtract one week from isoweek0_sunday and check if it's still in the same calendar
        // year
        let week_duration = chrono::Duration::weeks(1);
        let candidate_sunday = isoweek0_sunday.clone() - week_duration;

        let first_sunday_of_year = if candidate_sunday.year() == dt_value.year() {
            candidate_sunday
        } else {
            isoweek0_sunday
        };

        // Step 2: Find the ordinal date of the first sunday of the year
        let first_sunday_ordinal0 = first_sunday_of_year.ordinal0();

        // Step 3: Compare ordinal value of first sunday with that of dt_value
        let ordinal_delta = ordinal0 as i32 - first_sunday_ordinal0 as i32;

        // Compute how many whole weeks have passed since the first sunday of the year.
        // If date is prior to the first sunday in the calendar year, this will evaluate to -1.
        let week_number = (ordinal_delta as f64 / 7.0).floor() as i64;

        // Handle year truncation
        if !units_mask[0] {
            // Calendar year 2012. use weeks offset from the first Sunday of 2012
            // (which is January 1st)
            let first_sunday_of_2012 = in_tz
                .from_local_datetime(&NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2012, 1, 1).expect("invalid or out-of-range datetime"),
                    dt_value.time(),
                ))
                .earliest()
                .unwrap();

            dt_value = first_sunday_of_2012 + chrono::Duration::weeks(week_number);
        } else {
            // Don't change calendar year, use weeks offset from first sunday of the year
            dt_value = first_sunday_of_year + chrono::Duration::weeks(week_number);
        }
    } else if units_mask[5] {
        // Day
        // Keep weekday, but make sure Sunday comes before Monday
        let new_date = if weekday == Weekday::Sun {
            NaiveDate::from_isoywd_opt(dt_value.year(), 1, weekday)
        } else {
            NaiveDate::from_isoywd_opt(dt_value.year(), 2, weekday)
        }
        .expect("invalid or out-of-range datetime");

        let new_datetime = NaiveDateTime::new(new_date, dt_value.time());
        dt_value = in_tz.from_local_datetime(&new_datetime).earliest().unwrap();
    } else if units_mask[6] {
        // DayOfYear
        // Keep the same day of the year
        dt_value = dt_value.with_ordinal0(ordinal0).unwrap();
    } else {
        // Clear month and date
        dt_value = dt_value.with_ordinal0(0).unwrap();
    }

    dt_value
}

fn make_timeunit_start_udf() -> ScalarUDF {
    let timeunit_start: ScalarFunctionImplementation = Arc::new(|columns: &[ColumnarValue]| {
        let (timestamp, tz, units_mask) = unpack_timeunit_udf_args(columns)?;

        let array = timestamp.as_any().downcast_ref::<Int64Array>().unwrap();
        let result_array: TimestampMillisecondArray = unary(array, |value| {
            perform_timeunit_start_from_utc(value, units_mask.as_slice(), tz).timestamp_millis()
        });

        Ok(ColumnarValue::Array(Arc::new(result_array) as ArrayRef))
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_datatypes| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let make_sig = |timestamp_dtype: DataType| -> TypeSignature {
        TypeSignature::Exact(vec![
            timestamp_dtype,   // [0] timestamp
            DataType::Utf8,    // [1] timezone
            DataType::Boolean, // [2] Year
            DataType::Boolean, // [3] Quarter
            DataType::Boolean, // [4] Month
            DataType::Boolean, // [5] Date
            DataType::Boolean, // [6] Week
            DataType::Boolean, // [7] Day
            DataType::Boolean, // [8] DayOfYear
            DataType::Boolean, // [9] Hours
            DataType::Boolean, // [10] Minutes
            DataType::Boolean, // [11] Seconds
            DataType::Boolean, // [12] Milliseconds
        ])
    };

    let signature: Signature = Signature::one_of(
        vec![
            make_sig(DataType::Int64),
            make_sig(DataType::Date64),
            make_sig(DataType::Date32),
            make_sig(DataType::Timestamp(TimeUnit::Millisecond, None)),
            make_sig(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("vega_timeunit", &signature, &return_type, &timeunit_start)
}

lazy_static! {
    pub static ref TIMEUNIT_START_UDF: ScalarUDF = make_timeunit_start_udf();
}
