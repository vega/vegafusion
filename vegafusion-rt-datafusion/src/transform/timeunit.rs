use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DataFusionError};
use std::collections::HashSet;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::sql::dataframe::SqlDataFrame;

use crate::expression::compiler::builtin_functions::date_time::datetime::MAKE_TIMESTAMPTZ;

use crate::expression::compiler::builtin_functions::date_time::timestamp_to_timestamptz::TIMESTAMP_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::timestamptz_to_timestamp::TIMESTAMPTZ_TO_TIMESTAMP_UDF;
use crate::sql::compile::expr::ToSqlExpr;

use crate::expression::compiler::builtin_functions::date_time::epoch_to_timestamptz::EPOCH_MS_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use crate::expression::escape::{flat_col, unescaped_col};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday};
use datafusion_expr::expr::Cast;
use datafusion_expr::{
    floor, lit, BuiltinScalarFunction, ColumnarValue, Expr, ExprSchemable, ReturnTypeFunction,
    ScalarFunctionImplementation, ScalarUDF, Signature, TypeSignature, Volatility,
};
use itertools::Itertools;
use sqlgen::dialect::DialectDisplay;
use std::str::FromStr;
use vegafusion_core::arrow::array::{ArrayRef, Int64Array, TimestampMillisecondArray};
use vegafusion_core::arrow::compute::unary;
use vegafusion_core::arrow::temporal_conversions::date64_to_datetime;
use vegafusion_core::data::scalar::ScalarValue;

// Implementation of timeunit start using the SQL DATE_TRUNC function
fn timeunit_date_trunc(
    field: &str,
    smallest_unit: TimeUnitUnit,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, String)> {
    let (part_str, interval_str) = match smallest_unit {
        TimeUnitUnit::Year => ("year".to_string(), "1 YEAR".to_string()),
        TimeUnitUnit::Quarter => ("quarter".to_string(), "3 MONTH".to_string()),
        TimeUnitUnit::Month => ("month".to_string(), "1 MONTH".to_string()),
        TimeUnitUnit::Date => ("day".to_string(), "1 DAY".to_string()),
        TimeUnitUnit::Hours => ("hour".to_string(), "1 HOUR".to_string()),
        TimeUnitUnit::Minutes => ("minute".to_string(), "1 MINUTE".to_string()),
        TimeUnitUnit::Seconds => ("second".to_string(), "1 SECOND".to_string()),
        _ => {
            return Err(VegaFusionError::internal(format!(
                "Unsupported date trunc unit: {:?}",
                smallest_unit
            )))
        }
    };

    // Convert field column to timestamp
    let field_col = to_timestamp_col(field, schema, default_input_tz)?;

    let start_expr = if let Some(tz_str) = local_tz {
        let local_field = Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: vec![field_col, lit(tz_str.clone())],
        };

        let local_start_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DateTrunc,
            args: vec![lit(part_str), local_field],
        };

        Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMP_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![local_start_expr, lit(tz_str.clone())],
        }
    } else {
        // UTC, no timezone conversion needed
        Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DateTrunc,
            args: vec![lit(part_str), field_col],
        }
    };

    Ok((start_expr, interval_str))
}

// Implementation of timeunit start using MAKE_TIMESTAMPTZ and the SQL DATE_PART function
fn timeunit_date_part(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, String)> {
    // Initialize default arguments to make_timestamptz
    let mut make_timestamptz_args = vec![
        lit(2012), // 0 year
        lit(0),    // 1 month
        lit(1),    // 2 date
        lit(0),    // 3 hour
        lit(0),    // 4 minute
        lit(0),    // 5 second
        lit(0),    // 6 millisecond
        lit(local_tz.clone().unwrap_or_else(|| "UTC".to_string())),
    ];

    // Initialize interval string, this will be overwritten with the smallest specified unit
    let mut interval_str = "1 YEAR".to_string();

    // Convert field column to timestamp
    let field_col = to_timestamp_col(field, schema, default_input_tz)?;

    // Compute input timestamp expression based on timezone
    let inner = if let Some(tz_str) = local_tz {
        Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: vec![field_col, lit(tz_str.clone())],
        }
    } else {
        field_col
    };

    // Year
    if units_set.contains(&TimeUnitUnit::Year) {
        make_timestamptz_args[0] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("year"), inner.clone()],
        };

        interval_str = "1 YEAR".to_string();
    }

    // Quarter
    if units_set.contains(&TimeUnitUnit::Quarter) {
        let month = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("month"), inner.clone()],
        }
        .sub(lit(1.0));

        make_timestamptz_args[1] = Expr::Cast(Cast {
            expr: Box::new(floor(month.div(lit(3))).mul(lit(3))),
            data_type: DataType::Int64,
        });

        interval_str = "3 MONTH".to_string();
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        make_timestamptz_args[1] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("month"), inner.clone()],
        }
        .sub(lit(1.0));

        interval_str = "1 MONTH".to_string();
    }

    // Date
    if units_set.contains(&TimeUnitUnit::Date) {
        make_timestamptz_args[2] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("day"), inner.clone()],
        };

        interval_str = "1 DAY".to_string();
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        make_timestamptz_args[3] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("hour"), inner.clone()],
        };

        interval_str = "1 HOUR".to_string();
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        make_timestamptz_args[4] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("minute"), inner.clone()],
        };

        interval_str = "1 MINUTE".to_string();
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        make_timestamptz_args[5] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("second"), inner],
        };

        interval_str = "1 SECOND".to_string();
    }

    // Construct expression to make timestamp from components
    let start_expr = Expr::ScalarUDF {
        fun: Arc::new((*MAKE_TIMESTAMPTZ).clone()),
        args: make_timestamptz_args,
    };

    Ok((start_expr, interval_str))
}

fn to_timestamp_col(field: &str, schema: &DFSchema, default_input_tz: &String) -> Result<Expr> {
    let field_col = unescaped_col(field);
    Ok(match field_col.get_type(schema)? {
        DataType::Timestamp(_, _) => field_col,
        DataType::Utf8 => Expr::ScalarUDF {
            fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![field_col, lit(default_input_tz)],
        },
        dtype if is_numeric_datatype(&dtype) => Expr::ScalarUDF {
            fun: Arc::new((*EPOCH_MS_TO_TIMESTAMPTZ_UDF).clone()),
            args: vec![cast_to(field_col, &DataType::Int64, schema)?, lit("UTC")],
        },
        dtype => {
            return Err(VegaFusionError::compilation(format!(
                "Invalid data type for timeunit transform: {:?}",
                dtype
            )))
        }
    })
}

// timeunit transform for 'day' unit (day of the week)
fn timeunit_weekday(
    field: &str,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, String)> {
    let field_col = to_timestamp_col(field, schema, default_input_tz)?;

    // Compute input timestamp expression based on timezone
    let inner = if let Some(tz_str) = local_tz {
        Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: vec![field_col, lit(tz_str.clone())],
        }
    } else {
        field_col
    };

    // Use DATE_PART to extract the weekday
    // where Sunday is 0 and Saturday is 6
    let weekday0 = Expr::ScalarFunction {
        fun: BuiltinScalarFunction::DatePart,
        args: vec![lit("dow"), inner],
    };

    // Add one to line up with the signature of MAKE_TIMESTAMPTZ
    // where Sunday is 1 and Saturday is 7
    let weekday1 = weekday0.add(lit(1));

    // The year 2012 starts with a Sunday, so we can set the day of the month to match weekday1
    let make_timestamptz_args = vec![
        lit(2012), // 0 year
        lit(0),    // 1 month
        weekday1,  // 2 date
        lit(0),    // 3 hour
        lit(0),    // 4 minute
        lit(0),    // 5 second
        lit(0),    // 6 millisecond
        lit(local_tz.clone().unwrap_or_else(|| "UTC".to_string())),
    ];

    // Construct expression to make timestamp from components
    let start_expr = Expr::ScalarUDF {
        fun: Arc::new((*MAKE_TIMESTAMPTZ).clone()),
        args: make_timestamptz_args,
    };

    Ok((start_expr, "1 DAY".to_string()))
}

// Fallback implementation of timeunit that uses a custom DataFusion UDF
fn timeunit_custom_udf(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, String)> {
    let units_mask = vec![
        units_set.contains(&TimeUnitUnit::Year),         // 0
        units_set.contains(&TimeUnitUnit::Quarter),      // 1
        units_set.contains(&TimeUnitUnit::Month),        // 2
        units_set.contains(&TimeUnitUnit::Date),         // 3
        units_set.contains(&TimeUnitUnit::Week),         // 4
        units_set.contains(&TimeUnitUnit::Day),          // 5
        units_set.contains(&TimeUnitUnit::DayOfYear),    // 6
        units_set.contains(&TimeUnitUnit::Hours),        // 7
        units_set.contains(&TimeUnitUnit::Minutes),      // 8
        units_set.contains(&TimeUnitUnit::Seconds),      // 9
        units_set.contains(&TimeUnitUnit::Milliseconds), // 10
    ];

    let timeunit_start_udf = &TIMEUNIT_START_UDF;

    let local_tz = local_tz
        .as_ref()
        .map(|tz| tz.to_string())
        .unwrap_or_else(|| "UTC".to_string());

    let field_col = to_timestamp_col(field, schema, default_input_tz)?;

    let timeunit_start_value = timeunit_start_udf.call(vec![
        field_col,
        lit(local_tz),
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
    let mut interval_str = "".to_string();

    // Year
    if units_set.contains(&TimeUnitUnit::Year) {
        interval_str = "1 YEAR".to_string();
    }

    // Quarter
    if units_set.contains(&TimeUnitUnit::Quarter) {
        interval_str = "3 MONTH".to_string();
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        interval_str = "1 MONTH".to_string();
    }

    // Week
    if units_set.contains(&TimeUnitUnit::Week) {
        interval_str = "1 WEEK".to_string();
    }

    // Day
    if units_set.contains(&TimeUnitUnit::Date)
        || units_set.contains(&TimeUnitUnit::DayOfYear)
        || units_set.contains(&TimeUnitUnit::Day)
    {
        interval_str = "1 DAY".to_string();
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        interval_str = "1 HOUR".to_string();
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        interval_str = "1 MINUTE".to_string();
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        interval_str = "1 SECOND".to_string();
    }

    Ok((timeunit_start_value, interval_str))
}

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let tz_config = config
            .tz_config
            .with_context(|| "No local timezone info provided".to_string())?;

        let local_tz = if self.timezone != Some(TimeUnitTimeZone::Utc as i32) {
            Some(tz_config.local_tz)
        } else {
            None
        };

        let local_tz = local_tz.map(|tz| tz.to_string());
        let schema = dataframe.schema_df();
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
            .map(|unit_i32| TimeUnitUnit::from_i32(*unit_i32).unwrap())
            .collect::<Vec<TimeUnitUnit>>();

        // Add timeunit start
        let (timeunit_start_value, interval_str) = match *units_vec.as_slice() {
            [TimeUnitUnit::Year] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Year,
                &schema,
                &default_input_tz,
                &local_tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Quarter] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Quarter,
                &schema,
                &default_input_tz,
                &local_tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Month,
                &schema,
                &default_input_tz,
                &local_tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                &schema,
                &default_input_tz,
                &local_tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::DayOfYear] => timeunit_date_trunc(
                &self.field,
                TimeUnitUnit::Date,
                &schema,
                &default_input_tz,
                &local_tz,
            )?,
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Hours,
                    &schema,
                    &default_input_tz,
                    &local_tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Minutes,
                    &schema,
                    &default_input_tz,
                    &local_tz,
                )?
            }
            [TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes, TimeUnitUnit::Seconds] => {
                timeunit_date_trunc(
                    &self.field,
                    TimeUnitUnit::Seconds,
                    &schema,
                    &default_input_tz,
                    &local_tz,
                )?
            }
            [TimeUnitUnit::Day] => {
                timeunit_weekday(&self.field, &schema, &default_input_tz, &local_tz)?
            }
            _ => {
                // Check if timeunit can be handled by make_timestamptz
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
                    timeunit_date_part(
                        &self.field,
                        &units_set,
                        &schema,
                        &default_input_tz,
                        &local_tz,
                    )?
                } else {
                    // Fallback to custom UDF
                    timeunit_custom_udf(
                        &self.field,
                        &units_set,
                        &schema,
                        &default_input_tz,
                        &local_tz,
                    )?
                }
            }
        };

        let timeunit_start_value = timeunit_start_value.alias(&timeunit_start_alias);

        // Add timeunit start value to the dataframe
        let mut select_exprs: Vec<_> = dataframe
            .schema_df()
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
        select_exprs.push(timeunit_start_value);

        let dataframe = dataframe.select(select_exprs).await?;

        // Add timeunit end value to the dataframe
        let timeunit_end_alias = if let Some(alias_1) = &self.alias_1 {
            alias_1.clone()
        } else {
            "unit1".to_string()
        };
        let dialect = dataframe.dialect();
        let mut select_strs: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() != &timeunit_end_alias {
                    // Convert column name to string with dialect's rules for quoting
                    Some(
                        flat_col(field.name().as_str())
                            .to_sql()
                            .unwrap()
                            .sql(dialect)
                            .unwrap(),
                    )
                } else {
                    None
                }
            })
            .collect();

        if let Some(tz_str) = local_tz {
            // Apply offset in UTC
            select_strs.push(format!(
                "timestamp_to_timestamptz(timestamptz_to_timestamp({start}, '{tz}') + INTERVAL '{interval}', '{tz}') as {end}",
                start=flat_col(&timeunit_start_alias)
                    .to_sql()
                    .unwrap()
                    .sql(dialect)
                    .unwrap(),
                interval=interval_str,
                end=flat_col(&timeunit_end_alias)
                    .to_sql()
                    .unwrap()
                    .sql(dialect)
                    .unwrap(),
                tz=tz_str
            ));
        } else {
            select_strs.push(format!(
                "{start} + INTERVAL '{interval}' as {end}",
                start = flat_col(&timeunit_start_alias)
                    .to_sql()
                    .unwrap()
                    .sql(dialect)
                    .unwrap(),
                interval = interval_str,
                end = flat_col(&timeunit_end_alias)
                    .to_sql()
                    .unwrap()
                    .sql(dialect)
                    .unwrap(),
            ));
        }
        let select_csv = select_strs.join(", ");

        let dataframe = dataframe
            .chain_query_str(&format!(
                "SELECT {select_csv} from {parent}",
                select_csv = select_csv,
                parent = dataframe.parent_name()
            ))
            .await?;

        Ok((dataframe, Vec::new()))
    }
}

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
        DataFusionError::Internal(format!("Failed to parse {} as a timezone", tz_str))
    })?;

    let timestamp = columns[0].clone().into_array(1);
    let timestamp = process_input_datetime(&timestamp, &tz);

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

    let return_type: ReturnTypeFunction = Arc::new(move |_datatypes| {
        Ok(Arc::new(DataType::Timestamp(
            ArrowTimeUnit::Millisecond,
            None,
        )))
    });

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
            make_sig(DataType::Timestamp(ArrowTimeUnit::Millisecond, None)),
            make_sig(DataType::Timestamp(ArrowTimeUnit::Nanosecond, None)),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("vega_timeunit", &signature, &return_type, &timeunit_start)
}

lazy_static! {
    pub static ref TIMEUNIT_START_UDF: ScalarUDF = make_timeunit_start_udf();
}
