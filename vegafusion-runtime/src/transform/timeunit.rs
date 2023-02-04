use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_common::DFSchema;
use std::collections::HashSet;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use datafusion_expr::expr::Cast;
use datafusion_expr::{floor, lit, BuiltinScalarFunction, Expr, ExprSchemable};
use itertools::Itertools;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_datafusion_udfs::udfs::datetime::date_add::DATE_ADD_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::datetime_components::MAKE_TIMESTAMPTZ;
use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_timestamptz::EPOCH_MS_TO_TIMESTAMPTZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timestamp_to_timestamptz::TIMESTAMP_TO_TIMESTAMPTZ_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timestamptz_to_timestamp::TIMESTAMPTZ_TO_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timeunit::TIMEUNIT_START_UDF;

// Implementation of timeunit start using the SQL DATE_TRUNC function
fn timeunit_date_trunc(
    field: &str,
    smallest_unit: TimeUnitUnit,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, (i32, String))> {
    let (part_str, interval) = match smallest_unit {
        TimeUnitUnit::Year => ("year".to_string(), (1, "YEAR".to_string())),
        TimeUnitUnit::Quarter => ("quarter".to_string(), (3, "MONTH".to_string())),
        TimeUnitUnit::Month => ("month".to_string(), (1, "MONTH".to_string())),
        TimeUnitUnit::Date => ("day".to_string(), (1, "DAY".to_string())),
        TimeUnitUnit::Hours => ("hour".to_string(), (1, "HOUR".to_string())),
        TimeUnitUnit::Minutes => ("minute".to_string(), (1, "MINUTE".to_string())),
        TimeUnitUnit::Seconds => ("second".to_string(), (1, "SECOND".to_string())),
        _ => {
            return Err(VegaFusionError::internal(format!(
                "Unsupported date trunc unit: {smallest_unit:?}"
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

    Ok((start_expr, interval))
}

// Implementation of timeunit start using MAKE_TIMESTAMPTZ and the SQL DATE_PART function
fn timeunit_date_part(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, (i32, String))> {
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
    let mut interval = (1, "YEAR".to_string());

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

        interval = (1, "YEAR".to_string());
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

        interval = (3, "MONTH".to_string());
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        make_timestamptz_args[1] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("month"), inner.clone()],
        }
        .sub(lit(1.0));

        interval = (1, "MONTH".to_string());
    }

    // Date
    if units_set.contains(&TimeUnitUnit::Date) {
        make_timestamptz_args[2] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("day"), inner.clone()],
        };

        interval = (1, "DAY".to_string());
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        make_timestamptz_args[3] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("hour"), inner.clone()],
        };

        interval = (1, "HOUR".to_string());
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        make_timestamptz_args[4] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("minute"), inner.clone()],
        };

        interval = (1, "MINUTE".to_string());
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        make_timestamptz_args[5] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("second"), inner],
        };

        interval = (1, "SECOND".to_string());
    }

    // Construct expression to make timestamp from components
    let start_expr = Expr::ScalarUDF {
        fun: Arc::new((*MAKE_TIMESTAMPTZ).clone()),
        args: make_timestamptz_args,
    };

    Ok((start_expr, interval))
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
    local_tz: &Option<String>,
) -> Result<(Expr, (i32, String))> {
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

    Ok((start_expr, (1, "DAY".to_string())))
}

// Fallback implementation of timeunit that uses a custom DataFusion UDF
fn timeunit_custom_udf(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    schema: &DFSchema,
    default_input_tz: &String,
    local_tz: &Option<String>,
) -> Result<(Expr, (i32, String))> {
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
    let mut interval = (1, "YEAR".to_string());

    // Year
    if units_set.contains(&TimeUnitUnit::Year) {
        interval = (1, "YEAR".to_string());
    }

    // Quarter
    if units_set.contains(&TimeUnitUnit::Quarter) {
        interval = (3, "MONTH".to_string());
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        interval = (1, "MONTH".to_string());
    }

    // Week
    if units_set.contains(&TimeUnitUnit::Week) {
        interval = (1, "WEEK".to_string());
    }

    // Day
    if units_set.contains(&TimeUnitUnit::Date)
        || units_set.contains(&TimeUnitUnit::DayOfYear)
        || units_set.contains(&TimeUnitUnit::Day)
    {
        interval = (1, "DAY".to_string());
    }

    // Hour
    if units_set.contains(&TimeUnitUnit::Hours) {
        interval = (1, "HOUR".to_string());
    }

    // Minute
    if units_set.contains(&TimeUnitUnit::Minutes) {
        interval = (1, "MINUTE".to_string());
    }

    // Second
    if units_set.contains(&TimeUnitUnit::Seconds) {
        interval = (1, "SECOND".to_string());
    }

    Ok((timeunit_start_value, interval))
}

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let tz_config = config
            .tz_config
            .with_context(|| "No local timezone info provided".to_string())?;

        let local_tz = if self.timezone != Some(TimeUnitTimeZone::Utc as i32) {
            Some(tz_config.local_tz)
        } else {
            None
        };

        let local_tz = local_tz.map(|tz| tz.to_string());
        let schema = dataframe.schema_df()?;
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
        let (timeunit_start_expr, interval) = match *units_vec.as_slice() {
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

        let timeunit_start_expr = timeunit_start_expr.alias(&timeunit_start_alias);

        // Add timeunit start value to the dataframe
        let mut select_exprs: Vec<_> = dataframe
            .schema_df()?
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

        let dataframe = dataframe.select(select_exprs).await?;

        // Add timeunit end value to the dataframe
        let timeunit_end_alias = if let Some(alias_1) = &self.alias_1 {
            alias_1.clone()
        } else {
            "unit1".to_string()
        };

        let timeunit_end_expr = if let Some(tz_str) = local_tz {
            let ts = Expr::ScalarUDF {
                fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
                args: vec![flat_col(&timeunit_start_alias), lit(&tz_str)],
            };

            let ts_shifted = Expr::ScalarUDF {
                fun: Arc::new((*DATE_ADD_UDF).clone()),
                args: vec![lit(&interval.1), lit(interval.0), ts],
            };

            Expr::ScalarUDF {
                fun: Arc::new((*TIMESTAMP_TO_TIMESTAMPTZ_UDF).clone()),
                args: vec![ts_shifted, lit(&tz_str)],
            }
        } else {
            Expr::ScalarUDF {
                fun: Arc::new((*DATE_ADD_UDF).clone()),
                args: vec![
                    lit(&interval.1),
                    lit(interval.0),
                    flat_col(&timeunit_start_alias),
                ],
            }
        }
        .alias(&timeunit_end_alias);

        let mut select_exprs: Vec<_> = dataframe
            .schema_df()?
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
        let dataframe = dataframe.select(select_exprs).await?;

        Ok((dataframe, Vec::new()))
    }
}
