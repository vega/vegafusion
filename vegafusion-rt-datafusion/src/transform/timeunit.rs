/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::prelude::{col, DataFrame};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion::arrow::compute::kernels::arity::unary;
use datafusion::arrow::temporal_conversions::date64_to_datetime;

use chrono::{
    DateTime, Datelike, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday,
};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let _units: Vec<_> = self
            .units
            .clone()
            .into_iter()
            .map(|unit| TimeUnitUnit::from_i32(unit).unwrap())
            .collect();

        let is_local = self.timezone != Some(TimeUnitTimeZone::Utc as i32);

        let units_mask = vec![
            self.units.contains(&(TimeUnitUnit::Year as i32)), // 0
            self.units.contains(&(TimeUnitUnit::Quarter as i32)), // 1
            self.units.contains(&(TimeUnitUnit::Month as i32)), // 2
            self.units.contains(&(TimeUnitUnit::Date as i32)), // 3
            self.units.contains(&(TimeUnitUnit::Week as i32)), // 4
            self.units.contains(&(TimeUnitUnit::Day as i32)),  // 5
            self.units.contains(&(TimeUnitUnit::DayOfYear as i32)), // 6
            self.units.contains(&(TimeUnitUnit::Hours as i32)), // 7
            self.units.contains(&(TimeUnitUnit::Minutes as i32)), // 8
            self.units.contains(&(TimeUnitUnit::Seconds as i32)), // 9
            self.units.contains(&(TimeUnitUnit::Milliseconds as i32)), // 10
        ];

        // Handle timeunit start value (we always do this)
        let timeunit_start_udf = make_timeunit_start_udf(units_mask.as_slice(), is_local);
        let timeunit_start_value = timeunit_start_udf.call(vec![col(&self.field)]);

        // Apply alias
        let timeunit_start_alias = if let Some(alias_0) = &self.alias_0 {
            alias_0.clone()
        } else {
            "unit0".to_string()
        };
        let timeunit_start_value = timeunit_start_value.alias(&timeunit_start_alias);

        // Add timeunit start value to the dataframe
        let dataframe = dataframe.select(vec![Expr::Wildcard, timeunit_start_value])?;

        // Handle timeunit end value (In the future, disable this when interval=false)
        let timeunit_end_udf = make_timeunit_end_udf(units_mask.as_slice(), is_local);
        let timeunit_end_value = timeunit_end_udf.call(vec![col(&timeunit_start_alias)]);

        // Apply alias
        let timeunit_end_alias = if let Some(alias_1) = &self.alias_1 {
            alias_1.clone()
        } else {
            "unit1".to_string()
        };
        let timeunit_end_value = timeunit_end_value.alias(&timeunit_end_alias);

        // Add timeunit end value to the dataframe
        let dataframe = dataframe.select(vec![Expr::Wildcard, timeunit_end_value])?;

        Ok((dataframe.clone(), Vec::new()))
    }
}

fn make_timeunit_start_udf(units_mask: &[bool], in_local: bool) -> ScalarUDF {
    let units_mask = Vec::from(units_mask);
    let timeunit = move |args: &[ArrayRef]| {
        let arg = &args[0];

        // Input UTC
        let array = arg.as_any().downcast_ref::<Int64Array>().unwrap();
        let result_array: Int64Array = if in_local {
            // Input is in UTC, compute timeunit values in local, return results in UTC
            let tz = Local {};
            unary(array, |value| {
                perform_timeunit_start_from_utc(value, units_mask.as_slice(), tz).timestamp_millis()
            })
        } else {
            // Input is in UTC, compute timeunit values in UTC, return results in UTC
            let tz = Utc;
            unary(array, |value| {
                perform_timeunit_start_from_utc(value, units_mask.as_slice(), tz).timestamp_millis()
            })
        };

        Ok(Arc::new(result_array) as ArrayRef)
    };

    let timeunit = make_scalar_function(timeunit);
    let return_type: ReturnTypeFunction = Arc::new(move |_datatypes| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "timeunit",
        &Signature::uniform(1, vec![DataType::Int64], Volatility::Immutable),
        &return_type,
        &timeunit,
    )
}

fn make_timeunit_end_udf(units_mask: &[bool], in_local: bool) -> ScalarUDF {
    let units_mask = Vec::from(units_mask);
    let timeunit_end = move |args: &[ArrayRef]| {
        let arg = &args[0];

        let start_array = arg.as_any().downcast_ref::<Int64Array>().unwrap();
        let result_array: Int64Array = if in_local {
            let tz = Local {};
            unary(start_array, |value| {
                perform_timeunit_end_from_utc(value, units_mask.as_slice(), tz).timestamp_millis()
            })
        } else {
            let tz = Utc;
            unary(start_array, |value| {
                perform_timeunit_end_from_utc(value, units_mask.as_slice(), tz).timestamp_millis()
            })
        };

        Ok(Arc::new(result_array) as ArrayRef)
    };

    let timeunit = make_scalar_function(timeunit_end);
    let return_type: ReturnTypeFunction = Arc::new(move |_datatypes| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "timeunit_end",
        &Signature::uniform(1, vec![DataType::Int64], Volatility::Immutable),
        &return_type,
        &timeunit,
    )
}

/// For timestamp specified in UTC, perform time unit in the provided timezone (either UTC or Local)
fn perform_timeunit_start_from_utc<T: TimeZone>(
    value: i64,
    units_mask: &[bool],
    in_tz: T,
) -> DateTime<T> {
    // Load and interpret date time as UTC
    let dt_value = date64_to_datetime(value).with_nanosecond(0).unwrap();
    let dt_value = Utc.from_local_datetime(&dt_value).single().unwrap();

    let mut dt_value = dt_value.with_timezone(&in_tz);

    // Handle time truncation
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

    if !units_mask[7] {
        // Hours
        dt_value = dt_value.with_hour(0).unwrap();
    }

    // Save off day of the year and weekday here, becuase these will change if the
    // year is changed
    let ordinal0 = dt_value.ordinal0();
    let weekday = dt_value.weekday();

    // Handle year truncation
    // (if we're not truncating to week number, this is handled separately below)
    if !units_mask[0] && !units_mask[4] {
        // Year
        dt_value = dt_value.with_year(2012).unwrap();
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

        let isoweek0_sunday = NaiveDate::from_isoywd(dt_value.year(), 1, Weekday::Sun);

        let isoweek0_sunday = NaiveDateTime::new(isoweek0_sunday, dt_value.time());
        let isoweek0_sunday = in_tz
            .from_local_datetime(&isoweek0_sunday)
            .single()
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
                    NaiveDate::from_ymd(2012, 1, 1),
                    dt_value.time(),
                ))
                .single()
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
            NaiveDate::from_isoywd(dt_value.year(), 1, weekday)
        } else {
            NaiveDate::from_isoywd(dt_value.year(), 2, weekday)
        };
        let new_datetime = NaiveDateTime::new(new_date, dt_value.time());
        dt_value = in_tz.from_local_datetime(&new_datetime).single().unwrap();
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

/// For timestamp specified in UTC, perform time unit end in the provided timezone (either UTC or Local)
fn perform_timeunit_end_from_utc<T: TimeZone>(
    value: i64,
    units_mask: &[bool],
    tz: T,
) -> DateTime<T> {
    let dt_start = date64_to_datetime(value).with_nanosecond(0).unwrap();
    let dt_start = Utc.from_local_datetime(&dt_start).single().unwrap();
    let dt_start = dt_start.with_timezone(&tz);

    // create dt_end by advancing dt_start by the smallest unit present in units
    if units_mask[10] {
        // Milliseconds
        let delta = chrono::Duration::milliseconds(1);
        dt_start + delta
    } else if units_mask[9] {
        // Seconds
        let delta = chrono::Duration::seconds(1);
        dt_start + delta
    } else if units_mask[8] {
        // Minutes
        let delta = chrono::Duration::minutes(1);
        dt_start + delta
    } else if units_mask[7] {
        // Hours
        let delta = chrono::Duration::hours(1);
        dt_start + delta
    } else if units_mask[6] // DayOfYear
        || units_mask[5]    // Day
        || units_mask[3]
    // Date
    {
        let delta = chrono::Duration::days(1);
        dt_start + delta
    } else if units_mask[4] {
        // Week
        let delta = chrono::Duration::weeks(1);
        dt_start + delta
    } else if units_mask[2] {
        // Month
        let month0 = dt_start.month0();
        if month0 == 11 {
            // First day of the following year
            let year = dt_start.year();
            dt_start
                .with_ordinal0(0)
                .unwrap()
                .with_year(year + 1)
                .unwrap()
        } else {
            // Increment month
            dt_start
                .with_day0(0)
                .unwrap()
                .with_month0(month0 + 1)
                .unwrap()
        }
    } else if units_mask[1] {
        // Quarter
        let month0 = dt_start.month0();
        if month0 > 8 {
            // October 1st (or later, but month0 should have already been truncated to October 1st)
            // Wrap to start of the following year
            let year = dt_start.year();
            dt_start
                .with_ordinal0(0)
                .unwrap()
                .with_year(year + 1)
                .unwrap()
        } else {
            // Increment by 3 months (within the same year)
            dt_start
                .with_day0(0)
                .unwrap()
                .with_month0(month0 + 3)
                .unwrap()
        }
    } else if units_mask[0] {
        // Year
        // First day of the following year
        let year = dt_start.year();
        dt_start
            .with_ordinal0(0)
            .unwrap()
            .with_year(year + 1)
            .unwrap()
    } else {
        // Not unit specified, only thing to do is keep dt_start
        dt_start
    }
}
