use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::DFSchema;
use datafusion_functions::expr_fn::{date_part, date_trunc};
use std::collections::HashSet;
use std::ops::{Add, Mul, Rem, Sub};
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::datafusion::udfs::datetime::make_timestamptz::make_timestamptz;
use crate::datafusion::udfs::datetime::timeunit::TIMEUNIT_START_UDF;
use crate::expression::compiler::utils::ExprHelpers;
use crate::transform::utils::{from_epoch_millis, str_to_timestamp};
use datafusion_expr::{interval_datetime_lit, interval_year_month_lit, lit, Expr, ExprSchemable};
use itertools::Itertools;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};

/// Implementation of timeunit start using the SQL date_trunc function
fn timeunit_date_trunc(
    field: &str,
    smallest_unit: TimeUnitUnit,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    // Convert field to timestamp in target timezone
    let field_col = to_timestamp_col(unescaped_col(field), schema, default_input_tz)?.try_cast_to(
        &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz.into())),
        schema,
    )?;

    // Handle Sunday-based weeks as special case
    if let TimeUnitUnit::Week = smallest_unit {
        let day_interval = interval_datetime_lit("1 day");
        let trunc_expr =
            date_trunc(lit("week"), field_col.add(day_interval.clone())).sub(day_interval);
        let interval = interval_datetime_lit("7 day");
        return Ok((trunc_expr, interval));
    }

    // Handle uniform case
    let (part_str, interval_expr) = match smallest_unit {
        TimeUnitUnit::Year => ("year", interval_year_month_lit("1 year")),
        TimeUnitUnit::Quarter => ("quarter", interval_year_month_lit("3 month")),
        TimeUnitUnit::Month => ("month", interval_year_month_lit("1 month")),
        TimeUnitUnit::Date => ("day", interval_datetime_lit("1 day")),
        TimeUnitUnit::Hours => ("hour", interval_datetime_lit("1 hour")),
        TimeUnitUnit::Minutes => ("minute", interval_datetime_lit("1 minute")),
        TimeUnitUnit::Seconds => ("second", interval_datetime_lit("1 second")),
        TimeUnitUnit::Milliseconds => ("millisecond", interval_datetime_lit("1 millisecond")),
        _ => {
            return Err(VegaFusionError::internal(format!(
                "Unsupported date trunc unit: {smallest_unit:?}"
            )))
        }
    };

    // date_trunc after converting to the required timezone (will be the local_tz or UTC)
    let trunc_expr = date_trunc(lit(part_str), field_col);

    Ok((trunc_expr, interval_expr))
}

/// Implementation of timeunit start using make_timestamptz and the date_part functions
fn timeunit_date_part_tz(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    let mut year_arg = lit(2012);
    let mut month_arg = lit(1);
    let mut date_arg = lit(1);
    let mut hour_arg = lit(0);
    let mut minute_arg = lit(0);
    let mut second_arg = lit(0);
    let mut millisecond_arg = lit(0);

    // Initialize interval string, this will be overwritten with the smallest specified unit
    let mut interval = interval_year_month_lit("1 year");

    // Convert field column to timestamp
    let field_col = to_timestamp_col(unescaped_col(field), schema, default_input_tz)?.try_cast_to(
        &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz.into())),
        schema,
    )?;

    // Year
    if units_set.contains(&TimeUnitUnit::Year) {
        year_arg = date_part(lit("year"), field_col.clone());
        interval = interval_year_month_lit("1 year");
    }

    // Quarter
    if units_set.contains(&TimeUnitUnit::Quarter) {
        // Compute month (1-based) from the extracted quarter (1-based)
        let month_from_quarter = date_part(lit("quarter"), field_col.clone())
            .sub(lit(1))
            .mul(lit(3))
            .add(lit(1));

        month_arg = month_from_quarter;
        interval = interval_year_month_lit("3 month");
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        month_arg = date_part(lit("month"), field_col.clone());
        interval = interval_year_month_lit("1 month");
    }

    // Date
    if units_set.contains(&TimeUnitUnit::Date) {
        date_arg = date_part(lit("day"), field_col.clone());
        interval = interval_datetime_lit("1 day");
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        hour_arg = date_part(lit("hour"), field_col.clone());
        interval = interval_datetime_lit("1 hour");
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        minute_arg = date_part(lit("minute"), field_col.clone());
        interval = interval_datetime_lit("1 minute");
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        second_arg = date_part(lit("second"), field_col.clone());
        interval = interval_datetime_lit("1 second");
    }

    // Millisecond
    if units_set.contains(&TimeUnitUnit::Seconds) {
        millisecond_arg = date_part(lit("millisecond"), field_col.clone()).rem(lit(1000));
        interval = interval_datetime_lit("1 millisecond");
    }

    // Construct expression to make timestamp from components
    let start_expr = make_timestamptz(
        year_arg,
        month_arg,
        date_arg,
        hour_arg,
        minute_arg,
        second_arg,
        millisecond_arg,
        tz,
    );

    Ok((start_expr, interval))
}

/// timeunit transform for 'day' unit (day of the week)
fn timeunit_weekday(
    field: &str,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    let field_col = to_timestamp_col(unescaped_col(field), schema, default_input_tz)?.try_cast_to(
        &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz.into())),
        schema,
    )?;

    // Use DATE_PART_TZ to extract the weekday
    // where Sunday is 0, Saturday is 6
    let weekday0 = date_part(lit("dow"), field_col);

    // Add one to line up with the signature of make_timestamptz
    // where Sunday is 1 and Saturday is 7
    let weekday1 = weekday0.add(lit(1));

    let start_expr = make_timestamptz(
        lit(2012),
        lit(1),
        weekday1,
        lit(0),
        lit(0),
        lit(0),
        lit(0),
        tz,
    );

    Ok((start_expr, interval_datetime_lit("1 day")))
}

// Fallback implementation of timeunit that uses a custom DataFusion UDF
fn timeunit_custom_udf(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    let units_mask = [
        units_set.contains(&TimeUnitUnit::Year),      // 0
        units_set.contains(&TimeUnitUnit::Quarter),   // 1
        units_set.contains(&TimeUnitUnit::Month),     // 2
        units_set.contains(&TimeUnitUnit::Date),      // 3
        units_set.contains(&TimeUnitUnit::Week),      // 4
        units_set.contains(&TimeUnitUnit::Day),       // 5
        units_set.contains(&TimeUnitUnit::DayOfYear), // 6
        units_set.contains(&TimeUnitUnit::Hours),     // 7
        units_set.contains(&TimeUnitUnit::Minutes),   // 8
        units_set.contains(&TimeUnitUnit::Seconds),   // 9
        units_set.contains(&TimeUnitUnit::Milliseconds),
    ];

    let timeunit_start_udf = &TIMEUNIT_START_UDF;

    let field_col = to_timestamp_col(unescaped_col(field), schema, default_input_tz)?.try_cast_to(
        &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some("UTC".into())),
        schema,
    )?;

    let timeunit_start_value = timeunit_start_udf.call(vec![
        field_col,
        lit(tz),
        lit(units_mask[0]),
        lit(units_mask[1]),
        lit(units_mask[2]),
        lit(units_mask[3]),
        lit(units_mask[4]),
        lit(units_mask[5]),
        lit(units_mask[6]),
        lit(units_mask[7]),
        lit(units_mask[8]),
        lit(units_mask[9]),
        lit(units_mask[10]),
    ]);

    // Initialize interval string, this will be overwritten with the smallest specified unit
    let mut interval = interval_year_month_lit("1 year");

    // Year
    if units_set.contains(&TimeUnitUnit::Year) {
        interval = interval_year_month_lit("1 year");
    }

    // Quarter
    if units_set.contains(&TimeUnitUnit::Quarter) {
        interval = interval_year_month_lit("3 month");
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        interval = interval_year_month_lit("1 month");
    }

    // Week
    if units_set.contains(&TimeUnitUnit::Week) {
        interval = interval_datetime_lit("7 day");
    }

    // Day
    if units_set.contains(&TimeUnitUnit::Date)
        || units_set.contains(&TimeUnitUnit::DayOfYear)
        || units_set.contains(&TimeUnitUnit::Day)
    {
        interval = interval_datetime_lit("1 day");
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        interval = interval_datetime_lit("1 hour");
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        interval = interval_datetime_lit("1 minute");
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        interval = interval_datetime_lit("1 second");
    }

    Ok((timeunit_start_value, interval))
}

/// Convert a column to a timezone aware timestamp with Millisecond precision, in UTC
pub fn to_timestamp_col(expr: Expr, schema: &DFSchema, default_input_tz: &str) -> Result<Expr> {
    Ok(match expr.get_type(schema)? {
        DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(_)) => expr,
        DataType::Timestamp(_, Some(tz)) => expr.try_cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz)),
            schema,
        )?,
        DataType::Timestamp(_, None) => expr.try_cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(default_input_tz.into())),
            schema,
        )?,
        DataType::Date32 | DataType::Date64 => cast_to(
            expr,
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            schema,
        )?
        .try_cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(default_input_tz.into())),
            schema,
        )?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            str_to_timestamp(expr, default_input_tz, schema, None)?
        }
        dtype if is_numeric_datatype(&dtype) => from_epoch_millis(expr, schema)?,
        dtype => {
            return Err(VegaFusionError::compilation(format!(
                "Invalid data type for timeunit transform: {dtype:?}"
            )))
        }
    })
}

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let tz_config = config
            .tz_config
            .with_context(|| "No local timezone info provided".to_string())?;

        let tz = if self.timezone != Some(TimeUnitTimeZone::Utc as i32) {
            tz_config.local_tz.to_string()
        } else {
            "UTC".to_string()
        };

        let schema = dataframe.schema();
        let default_input_tz = tz_config.default_input_tz.to_string();

        // Compute Apply alias
        let timeunit_start_alias = if let Some(alias_0) = &self.alias_0 {
            alias_0.clone()
        } else {
            "unit0".to_string()
        };

        let units_vec = self
            .units
            .iter()
            .sorted()
            .map(|unit_i32| TimeUnitUnit::try_from(*unit_i32).unwrap())
            .collect::<Vec<TimeUnitUnit>>();

        // Add timeunit start
        let (timeunit_start_expr, interval) = match *units_vec.as_slice() {
            [TimeUnitUnit::Year] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Year,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Quarter] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Quarter,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Month,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Week] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Week,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::DayOfYear] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Hours,
                    schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Minutes,
                    schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes, TimeUnitUnit::Seconds] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Seconds,
                    schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Day] => timeunit_weekday(&self.field, schema, &default_input_tz, &tz)?,
            _ => {
                // Check if timeunit can be handled by make_utc_timestamp
                let units_set = units_vec.iter().cloned().collect::<HashSet<_>>();
                let date_part_units = vec![
                    TimeUnitUnit::Year,
                    TimeUnitUnit::Quarter,
                    TimeUnitUnit::Month,
                    TimeUnitUnit::Date,
                    TimeUnitUnit::Hours,
                    TimeUnitUnit::Minutes,
                    TimeUnitUnit::Seconds,
                ]
                .into_iter()
                .collect::<HashSet<_>>();
                if units_set.is_subset(&date_part_units) {
                    timeunit_date_part_tz(&self.field, &units_set, schema, &default_input_tz, &tz)?
                } else {
                    // Fallback to custom UDF
                    timeunit_custom_udf(&self.field, &units_set, schema, &default_input_tz, &tz)?
                }
            }
        };

        let timeunit_start_expr = timeunit_start_expr.alias(&timeunit_start_alias);

        // Add timeunit start value to the dataframe
        let mut select_exprs: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() != &timeunit_start_alias {
                    Some(flat_col(field.name()))
                } else {
                    None
                }
            })
            .collect();
        select_exprs.push(timeunit_start_expr);

        let dataframe = dataframe.select(select_exprs)?;

        // Add timeunit end value to the dataframe
        let timeunit_end_alias = if let Some(alias_1) = &self.alias_1 {
            alias_1.clone()
        } else {
            "unit1".to_string()
        };

        let timeunit_end_expr = flat_col(&timeunit_start_alias)
            .add(interval)
            .alias(&timeunit_end_alias);

        let mut select_exprs: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() != &timeunit_end_alias {
                    Some(flat_col(field.name()))
                } else {
                    None
                }
            })
            .collect();
        select_exprs.push(timeunit_end_expr);
        let dataframe = dataframe.select(select_exprs)?;

        Ok((dataframe, Vec::new()))
    }
}
