/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */

use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::expression::compiler::call::TzTransformFn;
use crate::task_graph::timezone::RuntimeTzConfig;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Timelike, Weekday};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr};

use datafusion::common::DataFusionError;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{
    lit, ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, Signature, TypeSignature,
    Volatility,
};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::error::Result;

#[inline(always)]
pub fn extract_year(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.year() as i64
}

#[inline(always)]
pub fn extract_month(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.month0() as i64
}

#[inline(always)]
pub fn extract_quarter(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    (dt.month0() as f64 / 3.0).floor() as i64 + 1
}

#[inline(always)]
pub fn extract_date(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.day() as i64
}

#[inline(always)]
pub fn extract_day(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    let weekday = dt.weekday();
    if matches!(weekday, Weekday::Sun) {
        0
    } else {
        weekday as i64 + 1
    }
}

#[inline(always)]
pub fn extract_dayofyear(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.ordinal() as i64
}

#[inline(always)]
pub fn extract_hour(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.hour() as i64
}

#[inline(always)]
pub fn extract_minute(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.minute() as i64
}

#[inline(always)]
pub fn extract_second(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.second() as i64
}

#[inline(always)]
pub fn extract_millisecond(dt: &DateTime<chrono_tz::Tz>) -> i64 {
    dt.nanosecond() as i64 / 1000000
}

pub fn make_tz_datepart_transform(udf: &ScalarUDF) -> TzTransformFn {
    let udf = udf.clone();
    let local_datepart_transform =
        move |tz_config: &RuntimeTzConfig, args: &[Expr], _schema: &DFSchema| -> Result<Expr> {
            let mut udf_args = vec![lit(tz_config.local_tz.to_string())];
            udf_args.extend(Vec::from(args));
            Ok(Expr::ScalarUDF {
                fun: Arc::new(udf.clone()),
                args: udf_args,
            })
        };
    Arc::new(local_datepart_transform)
}

pub fn make_datepart_udf(extract_fn: fn(&DateTime<chrono_tz::Tz>) -> i64, name: &str) -> ScalarUDF {
    let _inner_name = name.to_string();
    let part_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // Extract timezone string
        let tz_str = if let ColumnarValue::Scalar(tz_scalar) = &args[0] {
            tz_scalar.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected timezone to be a scalar".to_string(),
            ));
        };

        let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
            DataFusionError::Internal(format!("Failed to parse {} as a timezone", tz_str))
        })?;

        // Signature ensures there is a single argument
        let arg = args[1].clone().into_array(1);
        let arg = process_input_datetime(&arg, &tz);

        let mut result_builder = Int64Array::builder(arg.len());

        let arg = arg.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arg.len() {
            if arg.is_null(i) {
                result_builder.append_null();
            } else {
                let utc_millis = arg.value(i);
                let utc_seconds = utc_millis / 1_000;
                let utc_nanos = (utc_millis % 1_000 * 1_000_000) as u32;
                let naive_utc_datetime = NaiveDateTime::from_timestamp(utc_seconds, utc_nanos);
                let datetime: DateTime<chrono_tz::Tz> = tz.from_utc_datetime(&naive_utc_datetime);
                let value = extract_fn(&datetime);
                result_builder.append_value(value);
            }
        }

        let result = Arc::new(result_builder.finish()) as ArrayRef;

        // maybe back to scalar
        if arg.len() != 1 {
            Ok(ColumnarValue::Array(result))
        } else {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    let signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Date32]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Date64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new(name, &signature, &return_type, &part_fn)
}

lazy_static! {
    // Local UDFs
    pub static ref YEAR_UDF: ScalarUDF =
        make_datepart_udf(extract_year, "year");
    pub static ref MONTH_UDF: ScalarUDF =
        make_datepart_udf(extract_month, "month");
    pub static ref QUARTER_UDF: ScalarUDF =
        make_datepart_udf(extract_quarter, "quarter");
    pub static ref DATE_UDF: ScalarUDF =
        make_datepart_udf(extract_date, "date");
    pub static ref DAYOFYEAR_UDF: ScalarUDF =
        make_datepart_udf(extract_dayofyear, "dayofyear");
    pub static ref DAY_UDF: ScalarUDF =
        make_datepart_udf(extract_day, "day");
    pub static ref HOURS_UDF: ScalarUDF =
        make_datepart_udf(extract_hour, "hours");
    pub static ref MINUTES_UDF: ScalarUDF =
        make_datepart_udf(extract_minute, "minutes");
    pub static ref SECONDS_UDF: ScalarUDF =
        make_datepart_udf(extract_second, "seconds");
    pub static ref MILLISECONDS_UDF: ScalarUDF =
        make_datepart_udf(extract_millisecond, "milliseconds");

    // Local transforms
    pub static ref YEAR_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&YEAR_UDF);
    pub static ref MONTH_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&MONTH_UDF);
    pub static ref QUARTER_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&QUARTER_UDF);
    pub static ref DATE_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&DATE_UDF);
    pub static ref DAYOFYEAR_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&DAYOFYEAR_UDF);
    pub static ref DAY_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&DAY_UDF);
    pub static ref HOURS_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&HOURS_UDF);
    pub static ref MINUTES_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&MINUTES_UDF);
    pub static ref SECONDS_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&SECONDS_UDF);
    pub static ref MILLISECONDS_TRANSFORM: TzTransformFn =
        make_tz_datepart_transform(&MILLISECONDS_UDF);
}
