use chrono::{DateTime, Datelike, Local, LocalResult, TimeZone, Timelike, Utc, Weekday};
use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Date64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;
use vegafusion_core::arrow::compute::unary;
use crate::expression::compiler::builtin_functions::datetime::date_parsing::{DateParseMode, datetime_strs_to_millis};

#[inline(always)]
pub fn extract_year<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.year() as i64
}

#[inline(always)]
pub fn extract_month<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.month0() as i64
}

#[inline(always)]
pub fn extract_quarter<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    (dt.month0() as f64 / 3.0).floor() as i64 + 1
}

#[inline(always)]
pub fn extract_date<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.day() as i64
}

#[inline(always)]
pub fn extract_day<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    let weekday = dt.weekday();
    if matches!(weekday, Weekday::Sun) {
        0
    } else {
        weekday as i64 + 1
    }
}

#[inline(always)]
pub fn extract_dayofyear<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.ordinal() as i64
}

#[inline(always)]
pub fn extract_hour<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.hour() as i64
}

#[inline(always)]
pub fn extract_minute<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.minute() as i64
}

#[inline(always)]
pub fn extract_second<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.second() as i64
}

#[inline(always)]
pub fn extract_millisecond<T: TimeZone>(dt: &DateTime<T>) -> i64 {
    dt.nanosecond() as i64 / 1000000
}

pub fn make_datepart_udf_local(extract_fn: fn(&DateTime<Local>) -> i64, name: &str) -> ScalarUDF {
    let part_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let arg = match arg.data_type() {
            DataType::Utf8 => {
                let array = arg.as_any().downcast_ref::<StringArray>().unwrap();
                let millis_array = datetime_strs_to_millis(array, DateParseMode::JavaScript);
                cast(&millis_array, &DataType::Date64)?
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => cast(arg, &DataType::Date64)?,
            DataType::Date32 => {
                let ms_per_day = 1000 * 60 * 60 * 24 as i64;
                let array = arg.as_any().downcast_ref::<Date32Array>().unwrap();

                let array: Int64Array = unary(array, |v| (v as i64) * ms_per_day);
                let array = Arc::new(array) as ArrayRef;
                cast(&array, &DataType::Date64)?
            }
            DataType::Date64 => arg.clone(),
            DataType::Int64 => cast(arg, &DataType::Date64)?,
            _ => panic!("Unexpected data type for date part function:"),
        };

        let arg = arg.as_any().downcast_ref::<Date64Array>().unwrap();

        let mut result_builder = Int64Array::builder(arg.len());

        let local = Local {};
        let utc = Utc;

        for i in 0..arg.len() {
            if arg.is_null(i) {
                result_builder.append_null().unwrap();
            } else {
                match arg.value_as_datetime(i) {
                    Some(dt) => {
                        let utc_dt = match utc.from_local_datetime(&dt) {
                            LocalResult::None => {
                                result_builder.append_null().unwrap();
                                continue;
                            }
                            LocalResult::Single(dt) => dt,
                            LocalResult::Ambiguous(dt, _) => dt,
                        };
                        let local_dt = utc_dt.with_timezone(&local);
                        let value = extract_fn(&local_dt);
                        result_builder.append_value(value).unwrap();
                    }
                    None => {
                        result_builder.append_null().unwrap();
                    }
                }
            }
        }

        Ok(Arc::new(result_builder.finish()) as ArrayRef)
    };
    let part_fn = make_scalar_function(part_fn);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    ScalarUDF::new(
        name,
        &Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Date32,
                DataType::Date64,
                DataType::Int64,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &part_fn,
    )
}

pub fn make_datepart_udf_utc(extract_fn: fn(&DateTime<Utc>) -> i64, name: &str) -> ScalarUDF {
    let part_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];

        let arg = match arg.data_type() {
            DataType::Utf8 => {
                let array = arg.as_any().downcast_ref::<StringArray>().unwrap();
                let millis_array = datetime_strs_to_millis(array, DateParseMode::JavaScript);
                cast(&millis_array, &DataType::Date64)?
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => cast(arg, &DataType::Date64)?,
            DataType::Date32 => {
                let ms_per_day = 1000 * 60 * 60 * 24 as i64;
                let array = arg.as_any().downcast_ref::<Date32Array>().unwrap();

                let array: Int64Array = unary(array, |v| (v as i64) * ms_per_day);
                let array = Arc::new(array) as ArrayRef;
                cast(&array, &DataType::Date64)?
            }
            DataType::Date64 => arg.clone(),
            DataType::Int64 => cast(arg, &DataType::Date64)?,
            _ => panic!("Unexpected data type for date part function:"),
        };

        let arg = arg.as_any().downcast_ref::<Date64Array>().unwrap();

        let mut result_builder = Int64Array::builder(arg.len());
        let utc = Utc;

        for i in 0..arg.len() {
            if arg.is_null(i) {
                result_builder.append_null().unwrap();
            } else {
                match arg.value_as_datetime(i) {
                    Some(dt) => {
                        let utc_dt = match utc.from_local_datetime(&dt) {
                            LocalResult::None => {
                                result_builder.append_null().unwrap();
                                continue;
                            }
                            LocalResult::Single(dt) => dt,
                            LocalResult::Ambiguous(dt, _) => dt,
                        };
                        let value = extract_fn(&utc_dt);
                        result_builder.append_value(value).unwrap();
                    }
                    None => {
                        result_builder.append_null().unwrap();
                    }
                }
            }
        }

        Ok(Arc::new(result_builder.finish()) as ArrayRef)
    };
    let part_fn = make_scalar_function(part_fn);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));
    ScalarUDF::new(
        name,
        &Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Date32,
                DataType::Date64,
                DataType::Int64,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &part_fn,
    )
}

lazy_static! {
    pub static ref YEAR_UDF: ScalarUDF = make_datepart_udf_local(extract_year, "year");
    pub static ref MONTH_UDF: ScalarUDF = make_datepart_udf_local(extract_month, "month");
    pub static ref QUARTER_UDF: ScalarUDF = make_datepart_udf_local(extract_quarter, "quarter");
    pub static ref DATE_UDF: ScalarUDF = make_datepart_udf_local(extract_date, "date");
    pub static ref DAYOFYEAR_UDF: ScalarUDF =
        make_datepart_udf_local(extract_dayofyear, "dayofyear");
    pub static ref DAY_UDF: ScalarUDF = make_datepart_udf_local(extract_day, "day");
    pub static ref HOURS_UDF: ScalarUDF = make_datepart_udf_local(extract_hour, "hours");
    pub static ref MINUTES_UDF: ScalarUDF = make_datepart_udf_local(extract_minute, "minutes");
    pub static ref SECONDS_UDF: ScalarUDF = make_datepart_udf_local(extract_second, "seconds");
    pub static ref MILLISECONDS_UDF: ScalarUDF =
        make_datepart_udf_local(extract_millisecond, "milliseconds");
    pub static ref UTCYEAR_UDF: ScalarUDF = make_datepart_udf_utc(extract_year, "utcyear");
    pub static ref UTCMONTH_UDF: ScalarUDF = make_datepart_udf_utc(extract_month, "utcmonth");
    pub static ref UTCQUARTER_UDF: ScalarUDF = make_datepart_udf_utc(extract_quarter, "utcquarter");
    pub static ref UTCDATE_UDF: ScalarUDF = make_datepart_udf_utc(extract_date, "utcdate");
    pub static ref UTCDAYOFYEAR_UDF: ScalarUDF =
        make_datepart_udf_utc(extract_dayofyear, "utcdayofyear");
    pub static ref UTCDAY_UDF: ScalarUDF = make_datepart_udf_utc(extract_day, "utcday");
    pub static ref UTCHOURS_UDF: ScalarUDF = make_datepart_udf_utc(extract_hour, "utchours");
    pub static ref UTCMINUTES_UDF: ScalarUDF = make_datepart_udf_utc(extract_minute, "utcminutes");
    pub static ref UTCSECONDS_UDF: ScalarUDF = make_datepart_udf_utc(extract_second, "utcseconds");
    pub static ref UTCMILLISECONDS_UDF: ScalarUDF =
        make_datepart_udf_utc(extract_millisecond, "utcmilliseconds");
}
