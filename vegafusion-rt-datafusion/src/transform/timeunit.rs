use std::sync::Arc;
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::error::{Result, VegaFusionError};
use crate::transform::TransformTrait;
use datafusion::prelude::{DataFrame, col};
use crate::expression::compiler::config::CompilationConfig;
use vegafusion_core::task_graph::task_value::TaskValue;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Date64Array, Int64Array, TimestampNanosecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::TimeUnit as ArrowTimeUnit;
use datafusion::arrow::compute::kernels::arity::unary;
use datafusion::arrow::temporal_conversions::{date64_to_datetime, timestamp_ns_to_datetime};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::functions::{make_scalar_function, ReturnTypeFunction, Signature, Volatility};
use datafusion::physical_plan::udf::ScalarUDF;
use chrono::{Datelike, Timelike, Local, TimeZone, LocalResult, Utc, NaiveDate, Weekday, NaiveDateTime};
use crate::expression::compiler::utils::{cast_to, UNIT_SCHEMA};


#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {

        let units: Vec<_> = self.units.clone().into_iter().map(|unit| TimeUnitUnit::from_i32(unit).unwrap()).collect();

        let is_local = self.timezone != Some(TimeUnitTimeZone::Utc as i32);

        let units_mask = vec![
            self.units.contains(&(TimeUnitUnit::Year as i32)),          // 0
            self.units.contains(&(TimeUnitUnit::Quarter as i32)),       // 1
            self.units.contains(&(TimeUnitUnit::Month as i32)),         // 2
            self.units.contains(&(TimeUnitUnit::Date as i32)),          // 3
            self.units.contains(&(TimeUnitUnit::Week as i32)),          // 4
            self.units.contains(&(TimeUnitUnit::Day as i32)),           // 5
            self.units.contains(&(TimeUnitUnit::DayOfYear as i32)),     // 6
            self.units.contains(&(TimeUnitUnit::Hours as i32)),         // 7
            self.units.contains(&(TimeUnitUnit::Minutes as i32)),       // 8
            self.units.contains(&(TimeUnitUnit::Seconds as i32)),       // 9
            self.units.contains(&(TimeUnitUnit::Milliseconds as i32)),  // 10
        ];

        let timeunit = move |args: &[ArrayRef]| {
            let arg = &args[0];

            let array = arg
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap();

            let result_array: Int64Array = unary(array, |value| {
                // Load and interpret date time as UTC
                let dt_value = date64_to_datetime(value).with_nanosecond(0).unwrap();
                let dt_value = Utc.from_local_datetime(&dt_value).single().unwrap();

                if is_local {
                    let local = Local {};
                    let mut dt_value = dt_value.with_timezone(&local);

                    // Handle time truncation
                    if !units_mask[10] {  // Milliseconds
                        let new_ns = (((dt_value.nanosecond() as f64) / 1e6).floor() * 1e6) as u32;
                        dt_value = dt_value.with_nanosecond(new_ns).unwrap();
                    }

                    if !units_mask[9] {  // Seconds
                        dt_value = dt_value.with_second(0).unwrap();
                    }

                    if !units_mask[8] {  // Minutes
                        dt_value = dt_value.with_minute(0).unwrap();
                    }

                    if !units_mask[7] {  // Hours
                        dt_value = dt_value.with_hour(0).unwrap();
                    }

                    // Save off day of the year and weekday here, becuase these will change if the
                    // year is changed
                    let ordinal0 = dt_value.ordinal0();
                    let weekday = dt_value.weekday();

                    // Handle year truncation
                    if !units_mask[0] {  // Year
                        dt_value = dt_value.with_year(2012).unwrap();
                    }

                    // Handle date (of the year) truncation.
                    // For simplicity, only one of these is valid at the same time for now
                    if units_mask[1] {  // Quarter
                        // Truncate to Quarter
                        let new_month = ((dt_value.month0() as f64 / 3.0).floor() * 3.0) as u32;
                        println!("dt_value: {}, new_month0: {}", dt_value, new_month);
                        dt_value = dt_value.with_day0(0).unwrap().with_month0(new_month).unwrap();
                    } else if units_mask[2] {  // Month
                        // Truncate to first day of the month
                        dt_value = dt_value.with_day0(0).unwrap();
                    } else if units_mask[3] {  // Date
                        // Normalize to January, keeping existing day of the month.
                        // (January has 31 days, so this is safe)
                        dt_value = dt_value.with_month0(0).unwrap();
                    } else if units_mask[4] {  // Week
                        todo!("Week time unit not supported")
                    } else if units_mask[5] {  // Day
                        // Keep weekday, but make sure Sunday comes before Monday
                        let new_date = if weekday == Weekday::Sun {
                            NaiveDate::from_isoywd(dt_value.year(), 1, weekday)
                        } else {
                            NaiveDate::from_isoywd(dt_value.year(), 2, weekday)
                        };
                        let new_datetime = NaiveDateTime::new(new_date, dt_value.time());
                        dt_value = local.from_local_datetime(&new_datetime).single().unwrap();
                    } else if units_mask[6] {  // DayOfYear
                        // Keep the same day of the year
                        dt_value = dt_value.with_ordinal0(ordinal0).unwrap();
                    }

                    dt_value.timestamp_millis()
                } else {
                    let mut dt_value = dt_value;

                    // Handle time truncation
                    if !units_mask[10] {  // Milliseconds
                        let new_ns = (((dt_value.nanosecond() as f64) / 1e6).floor() * 1e6) as u32;
                        dt_value = dt_value.with_nanosecond(new_ns).unwrap();
                    }

                    if !units_mask[9] {  // Seconds
                        dt_value = dt_value.with_second(0).unwrap();
                    }

                    if !units_mask[8] {  // Minutes
                        dt_value = dt_value.with_minute(0).unwrap();
                    }

                    if !units_mask[7] {  // Hours
                        dt_value = dt_value.with_hour(0).unwrap();
                    }

                    // Save off day of the year and weekday here, becuase these will change if the
                    // year is changed
                    let ordinal0 = dt_value.ordinal0();
                    let weekday = dt_value.weekday();

                    // Handle year truncation
                    if !units_mask[0] {  // Year
                        dt_value = dt_value.with_year(2012).unwrap();
                    }

                    // Handle date (of the year) truncation.
                    // For simplicity, only one of these is valid at the same time for now
                    if units_mask[1] {  // Quarter
                        // Truncate to Quarter
                        let new_month = ((dt_value.month0() as f64 / 3.0).floor() * 3.0) as u32;
                        println!("dt_value: {}, new_month0: {}", dt_value, new_month);
                        dt_value = dt_value.with_day0(0).unwrap().with_month0(new_month).unwrap();
                    } else if units_mask[2] {  // Month
                        // Truncate to first day of the month
                        dt_value = dt_value.with_day0(0).unwrap();
                    } else if units_mask[3] {  // Date
                        // Normalize to January, keeping existing day of the month.
                        // (January has 31 days, so this is safe)
                        dt_value = dt_value.with_month0(0).unwrap();
                    } else if units_mask[4] {  // Week
                        todo!("Week time unit not supported")
                    } else if units_mask[5] {  // Day
                        // Keep weekday, but make sure Sunday comes before Monday
                        let new_date = if weekday == Weekday::Sun {
                            NaiveDate::from_isoywd(dt_value.year(), 1, weekday)
                        } else {
                            NaiveDate::from_isoywd(dt_value.year(), 2, weekday)
                        };
                        let new_datetime = NaiveDateTime::new(new_date, dt_value.time());
                        dt_value = Utc.from_local_datetime(&new_datetime).single().unwrap();
                    } else if units_mask[6] {  // DayOfYear
                        // Keep the same day of the year
                        dt_value = dt_value.with_ordinal0(ordinal0).unwrap();
                    }

                    dt_value.timestamp_millis()
                }
            });

            Ok(Arc::new(result_array) as ArrayRef)
        };

        let timeunit = make_scalar_function(timeunit);
        let return_type: ReturnTypeFunction = Arc::new(
            // move |_| Ok(Arc::new(DataType::Date64))
            move |_| Ok(Arc::new(DataType::Int64))
        );

        let timeunit_udf = ScalarUDF::new(
            "timeunit",
            &Signature::uniform(
                1,
                vec![DataType::Date64],
                Volatility::Immutable,
            ),
            &return_type,
            &timeunit,
        );

        let timeunit_value = timeunit_udf.call(vec![
            cast_to(col(&self.field), &DataType::Date64, dataframe.schema()).unwrap()
        ]);

        // Apply alias
        let timeunit_value = if let Some(alias_0) = &self.alias_0 {
            timeunit_value.alias(alias_0)
        } else {
            timeunit_value.alias("unit0")
        };

        let dataframe = dataframe
            .select(vec![Expr::Wildcard, timeunit_value])?;

        Ok((dataframe.clone(), Vec::new()))
    }
}
