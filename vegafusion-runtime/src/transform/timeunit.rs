use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_common::DFSchema;
use datafusion_functions::expr_fn::{date_trunc, floor, from_unixtime, to_timestamp_millis};
use std::collections::HashSet;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion_expr::expr::Cast;
use datafusion_expr::{expr, lit, Expr, ExprSchemable, interval_year_month_lit, interval_datetime_lit};
use itertools::Itertools;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};
use vegafusion_core::arrow::compute::kernels::cast_utils::parse_interval_day_time;
use vegafusion_datafusion_udfs::udfs::datetime::date_add_tz::DATE_ADD_TZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::date_part_tz::DATE_PART_TZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::date_trunc_tz::DATE_TRUNC_TZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_utc_timestamp::EPOCH_MS_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::make_utc_timestamp::MAKE_UTC_TIMESTAMP;
use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timeunit::TIMEUNIT_START_UDF;
use crate::transform::utils::{make_timestamp_parse_formats, str_to_timestamp};

// Implementation of timeunit start using the SQL DATE_TRUNC function
fn timeunit_date_trunc(
    field: &str,
    smallest_unit: TimeUnitUnit,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    let (part_str, interval_expr) = match smallest_unit {
        TimeUnitUnit::Year => ("year", interval_year_month_lit("1 year")),
        TimeUnitUnit::Quarter => ("quarter", interval_year_month_lit("1 quarter")),
        TimeUnitUnit::Month => ("month", interval_year_month_lit("1 quarter")),
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

    // Convert field to timestamp
    let field_col = to_timestamp_col(field, schema, default_input_tz)?;

    // date_trunc after converting to the required timezone (will be the local_tz or UTC)
    let trunc_expr = date_trunc(lit(part_str), field_col.cast_to(
        &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz.into())),
        schema
    )?);

    Ok((trunc_expr, interval_expr))
}

// Implementation of timeunit start using MAKE_UTC_TIMESTAMP and the DATE_PART_TZ function
fn timeunit_date_part_tz(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    todo!()
    // // Initialize default arguments to make_utc_timestamp
    // let mut make_timestamptz_args = vec![
    //     lit(2012), // 0 year
    //     lit(0),    // 1 month
    //     lit(1),    // 2 date
    //     lit(0),    // 3 hour
    //     lit(0),    // 4 minute
    //     lit(0),    // 5 second
    //     lit(0),    // 6 millisecond
    //     lit(local_tz.clone().unwrap_or_else(|| "UTC".to_string())),
    // ];
    //
    // // Initialize interval string, this will be overwritten with the smallest specified unit
    // let mut interval = (1, "YEAR".to_string());
    //
    // // Convert field column to timestamp
    // let field_col = to_timestamp_col(field, schema, default_input_tz)?;
    //
    // // Compute input timestamp expression based on timezone
    // let tz_str = local_tz.clone().unwrap_or_else(|| "UTC".to_string());
    //
    // // Year
    // if units_set.contains(&TimeUnitUnit::Year) {
    //     make_timestamptz_args[0] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("year"), field_col.clone(), lit(&tz_str)],
    //     });
    //
    //     interval = (1, "YEAR".to_string());
    // }
    //
    // // Quarter
    // if units_set.contains(&TimeUnitUnit::Quarter) {
    //     let month = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("month"), field_col.clone(), lit(&tz_str)],
    //     })
    //     .sub(lit(1.0));
    //
    //     make_timestamptz_args[1] = Expr::Cast(Cast {
    //         expr: Box::new(floor(month.div(lit(3))).mul(lit(3))),
    //         data_type: DataType::Int64,
    //     });
    //
    //     interval = (3, "MONTH".to_string());
    // }
    //
    // // Month
    // if units_set.contains(&TimeUnitUnit::Month) {
    //     make_timestamptz_args[1] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("month"), field_col.clone(), lit(&tz_str)],
    //     })
    //     .sub(lit(1.0));
    //
    //     interval = (1, "MONTH".to_string());
    // }
    //
    // // Date
    // if units_set.contains(&TimeUnitUnit::Date) {
    //     make_timestamptz_args[2] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("day"), field_col.clone(), lit(&tz_str)],
    //     });
    //
    //     interval = (1, "DAY".to_string());
    // }
    //
    // // Hour
    // if units_set.contains(&TimeUnitUnit::Hours) {
    //     make_timestamptz_args[3] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("hour"), field_col.clone(), lit(&tz_str)],
    //     });
    //
    //     interval = (1, "HOUR".to_string());
    // }
    //
    // // Minute
    // if units_set.contains(&TimeUnitUnit::Minutes) {
    //     make_timestamptz_args[4] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("minute"), field_col.clone(), lit(&tz_str)],
    //     });
    //
    //     interval = (1, "MINUTE".to_string());
    // }
    //
    // // Second
    // if units_set.contains(&TimeUnitUnit::Seconds) {
    //     make_timestamptz_args[5] = Expr::ScalarFunction(expr::ScalarFunction {
    //         func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //         args: vec![lit("second"), field_col, lit(&tz_str)],
    //     });
    //
    //     interval = (1, "SECOND".to_string());
    // }
    //
    // // Construct expression to make timestamp from components
    // let start_expr = Expr::ScalarFunction(expr::ScalarFunction {
    //     func: Arc::new((*MAKE_UTC_TIMESTAMP).clone()),
    //     args: make_timestamptz_args,
    // });
    //
    // Ok((start_expr, interval))
}

/// Convert a column to a timezone aware timestamp with Millisecond precision
fn to_timestamp_col(field: &str, schema: &DFSchema, default_input_tz: &String) -> Result<Expr> {
    let field_col = unescaped_col(field);
    Ok(match field_col.get_type(schema)? {
        DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(_)) => field_col,
        DataType::Timestamp(_, Some(tz)) => field_col.cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(tz)),
            schema
        )?,
        DataType::Timestamp(_, None) => field_col.cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(default_input_tz.as_str().into())),
            schema
        )?,
        DataType::Date32 | DataType::Date64 => cast_to(
            field_col,
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            schema,
        )?.cast_to(
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(default_input_tz.as_str().into())),
            schema
        )?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            str_to_timestamp(field_col, default_input_tz, schema)?
        }
        dtype if is_numeric_datatype(&dtype) => {
            // Convert to timestamp then localize to UTC
            let nanos = field_col * lit(1e6);
            from_unixtime(nanos).cast_to(
                &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some("UTC".into())),
                schema
            )?
        },
        dtype => {
            return Err(VegaFusionError::compilation(format!(
                "Invalid data type for timeunit transform: {dtype:?}"
            )))
        }
    })
}

// timeunit transform for 'day' unit (day of the week)
fn timeunit_weekday(
    field: &str,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    todo!()
    // let field_col = to_timestamp_col(field, schema, default_input_tz)?;
    //
    // // Compute input timestamp expression based on timezone
    // let tz_str = local_tz.clone().unwrap_or_else(|| "UTC".to_string());
    //
    // // Use DATE_PART_TZ to extract the weekday
    // // where Sunday is 0 and Saturday is 6
    // let weekday0 = Expr::ScalarFunction(expr::ScalarFunction {
    //     func: Arc::new(DATE_PART_TZ_UDF.clone()),
    //     args: vec![lit("dow"), field_col, lit(tz_str)],
    // });
    //
    // // Add one to line up with the signature of MAKE_UTC_TIMESTAMP
    // // where Sunday is 1 and Saturday is 7
    // let weekday1 = weekday0.add(lit(1));
    //
    // // The year 2012 starts with a Sunday, so we can set the day of the month to match weekday1
    // let make_timestamptz_args = vec![
    //     lit(2012), // 0 year
    //     lit(0),    // 1 month
    //     weekday1,  // 2 date
    //     lit(0),    // 3 hour
    //     lit(0),    // 4 minute
    //     lit(0),    // 5 second
    //     lit(0),    // 6 millisecond
    //     lit(local_tz.clone().unwrap_or_else(|| "UTC".to_string())),
    // ];
    //
    // // Construct expression to make timestamp from components
    // let start_expr = Expr::ScalarFunction(expr::ScalarFunction {
    //     func: Arc::new((*MAKE_UTC_TIMESTAMP).clone()),
    //     args: make_timestamptz_args,
    // });
    //
    // Ok((start_expr, (1, "DAY".to_string())))
}

// Fallback implementation of timeunit that uses a custom DataFusion UDF
fn timeunit_custom_udf(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    tz: &str,
) -> Result<(Expr, Expr)> {
    todo!()
    // let units_mask = [
    //     units_set.contains(&TimeUnitUnit::Year),      // 0
    //     units_set.contains(&TimeUnitUnit::Quarter),   // 1
    //     units_set.contains(&TimeUnitUnit::Month),     // 2
    //     units_set.contains(&TimeUnitUnit::Date),      // 3
    //     units_set.contains(&TimeUnitUnit::Week),      // 4
    //     units_set.contains(&TimeUnitUnit::Day),       // 5
    //     units_set.contains(&TimeUnitUnit::DayOfYear), // 6
    //     units_set.contains(&TimeUnitUnit::Hours),     // 7
    //     units_set.contains(&TimeUnitUnit::Minutes),   // 8
    //     units_set.contains(&TimeUnitUnit::Seconds),   // 9
    //     units_set.contains(&TimeUnitUnit::Milliseconds),
    // ];
    //
    // let timeunit_start_udf = &TIMEUNIT_START_UDF;
    //
    // let field_col = to_timestamp_col(field, schema, default_input_tz)?;
    //
    // let timeunit_start_value = timeunit_start_udf.call(vec![
    //     field_col,
    //     lit(tz),
    //     lit(units_mask[0]),
    //     lit(units_mask[1]),
    //     lit(units_mask[2]),
    //     lit(units_mask[3]),
    //     lit(units_mask[4]),
    //     lit(units_mask[5]),
    //     lit(units_mask[6]),
    //     lit(units_mask[7]),
    //     lit(units_mask[8]),
    //     lit(units_mask[9]),
    //     lit(units_mask[10]),
    // ]);
    //
    // // Initialize interval string, this will be overwritten with the smallest specified unit
    // let mut interval = (1, "YEAR".to_string());
    //
    // // Year
    // if units_set.contains(&TimeUnitUnit::Year) {
    //     interval = (1, "YEAR".to_string());
    // }
    //
    // // Quarter
    // if units_set.contains(&TimeUnitUnit::Quarter) {
    //     interval = (3, "MONTH".to_string());
    // }
    //
    // // Month
    // if units_set.contains(&TimeUnitUnit::Month) {
    //     interval = (1, "MONTH".to_string());
    // }
    //
    // // Week
    // if units_set.contains(&TimeUnitUnit::Week) {
    //     interval = (1, "WEEK".to_string());
    // }
    //
    // // Day
    // if units_set.contains(&TimeUnitUnit::Date)
    //     || units_set.contains(&TimeUnitUnit::DayOfYear)
    //     || units_set.contains(&TimeUnitUnit::Day)
    // {
    //     interval = (1, "DAY".to_string());
    // }
    //
    // // Hour
    // if units_set.contains(&TimeUnitUnit::Hours) {
    //     interval = (1, "HOUR".to_string());
    // }
    //
    // // Minute
    // if units_set.contains(&TimeUnitUnit::Minutes) {
    //     interval = (1, "MINUTE".to_string());
    // }
    //
    // // Second
    // if units_set.contains(&TimeUnitUnit::Seconds) {
    //     interval = (1, "SECOND".to_string());
    // }
    //
    // Ok((timeunit_start_value, interval))
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
                &schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Quarter] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Quarter,
                &schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Month,
                &schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                &schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::DayOfYear] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                &schema,
                &default_input_tz,
                &tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Hours,
                    &schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Minutes,
                    &schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes, TimeUnitUnit::Seconds] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Seconds,
                    &schema,
                    &default_input_tz,
                    &tz,
                )?
            }
            [TimeUnitUnit::Day] => {
                timeunit_weekday(&self.field, &schema, &default_input_tz, &tz)?
            }
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
                    timeunit_date_part_tz(
                        &self.field,
                        &units_set,
                        &schema,
                        &default_input_tz,
                        &tz,
                    )?
                } else {
                    // Fallback to custom UDF
                    timeunit_custom_udf(
                        &self.field,
                        &units_set,
                        &schema,
                        &default_input_tz,
                        &tz,
                    )?
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
            .alias(&timeunit_end_alias);;

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
