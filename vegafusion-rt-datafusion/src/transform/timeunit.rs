/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use datafusion::prelude::col;
use std::collections::HashSet;
use std::ops::{Div, Mul, Sub};
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use vegafusion_core::task_graph::task_value::TaskValue;

use datafusion::arrow::compute::kernels::arity::unary;
use datafusion::arrow::temporal_conversions::date64_to_datetime;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday};
use datafusion::common::{DataFusionError, ScalarValue};

use crate::sql::dataframe::SqlDataFrame;

use crate::expression::compiler::builtin_functions::date_time::datetime::MAKE_TIMESTAMPTZ;
use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::expression::compiler::builtin_functions::date_time::timestamp_to_timestamptz::TIMESTAMP_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::timestamptz_to_timestamp::TIMESTAMPTZ_TO_TIMESTAMP_UDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::BuiltinScalarFunction::Exp;
use datafusion_expr::{
    floor, lit, BuiltinScalarFunction, ColumnarValue, Expr, ReturnTypeFunction,
    ScalarFunctionImplementation, Signature, TypeSignature, Volatility,
};
use itertools::Itertools;
use std::str::FromStr;
use vegafusion_core::arrow::array::TimestampMillisecondArray;

// Implementation of timeunit start using the SQL DATE_TRUNC function
fn date_trunc(
    field: &str,
    smallest_unit: TimeUnitUnit,
    tz_str: &Option<String>,
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

    let start_expr = if let Some(tz_str) = tz_str {
        let local_field = Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: vec![col(field), lit(tz_str.clone())],
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
            args: vec![lit(part_str), col(field)],
        }
    };

    Ok((start_expr, interval_str))
}

// Implementation of timeunit start using MAKE_TIMESTAMPTZ and the SQL DATE_PART function
fn date_part(
    field: &str,
    units_set: &HashSet<TimeUnitUnit>,
    tz_str: &Option<String>,
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
        lit(tz_str.clone().unwrap_or_else(|| "UTC".to_string())),
    ];

    // Initialize interval string, this will be overwritten with the smallest specified unit
    let mut interval_str = "1 YEAR".to_string();

    // Compute input timestamp expression based on timezone
    let inner = if let Some(tz_str) = tz_str {
        Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: vec![col(field), lit(tz_str.clone())],
        }
    } else {
        col(field)
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
        .sub(lit(1));

        make_timestamptz_args[1] = Expr::Cast {
            expr: Box::new(floor(month.div(lit(3))).mul(lit(3))),
            data_type: DataType::Int64,
        };

        interval_str = "3 MONTH".to_string();
    }

    // Month
    if units_set.contains(&TimeUnitUnit::Month) {
        make_timestamptz_args[1] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("month"), inner.clone()],
        }
        .sub(lit(1));

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
        make_timestamptz_args[4] = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit("second"), inner.clone()],
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

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let local_tz = if self.timezone != Some(TimeUnitTimeZone::Utc as i32) {
            Some(
                config
                    .tz_config
                    .with_context(|| "No local timezone info provided".to_string())?
                    .local_tz,
            )
        } else {
            None
        };
        let tz_str = local_tz.map(|tz| tz.to_string());

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
        let (timeunit_start_value, interval_str) = match units_vec.as_slice() {
            &[TimeUnitUnit::Year] => date_trunc(&self.field, TimeUnitUnit::Year, &tz_str)?,
            &[TimeUnitUnit::Year, TimeUnitUnit::Quarter] => {
                date_trunc(&self.field, TimeUnitUnit::Quarter, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::Month] => {
                date_trunc(&self.field, TimeUnitUnit::Month, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date] => {
                date_trunc(&self.field, TimeUnitUnit::Date, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::DayOfYear] => {
                date_trunc(&self.field, TimeUnitUnit::Date, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours] => {
                date_trunc(&self.field, TimeUnitUnit::Hours, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes] => {
                date_trunc(&self.field, TimeUnitUnit::Minutes, &tz_str)?
            }
            &[TimeUnitUnit::Year, TimeUnitUnit::Month, TimeUnitUnit::Date, TimeUnitUnit::Hours, TimeUnitUnit::Minutes, TimeUnitUnit::Seconds] => {
                date_trunc(&self.field, TimeUnitUnit::Seconds, &tz_str)?
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
                    date_part(&self.field, &units_set, &tz_str)?
                } else {
                    return Err(VegaFusionError::internal(format!(
                        "Unsupported combination of timeunit units: {:?}",
                        units_vec
                    )));
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
                    Some(col(field.name()))
                } else {
                    None
                }
            })
            .collect();
        select_exprs.push(timeunit_start_value);

        let dataframe = dataframe.select(select_exprs)?;

        // Add timeunit end value to the dataframe
        let timeunit_end_alias = if let Some(alias_1) = &self.alias_1 {
            alias_1.clone()
        } else {
            "unit1".to_string()
        };
        let mut select_strs: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() != &timeunit_end_alias {
                    Some(field.name().clone())
                } else {
                    None
                }
            })
            .collect();
        select_strs.push(format!(
            "{} + INTERVAL '{}' as {}",
            timeunit_start_alias, interval_str, timeunit_end_alias
        ));
        let select_csv = select_strs.join(", ");

        let dataframe = dataframe.chain_query_str(&format!(
            "SELECT {select_csv} from {parent}",
            select_csv = select_csv,
            parent = dataframe.parent_name()
        ))?;

        Ok((dataframe, Vec::new()))
    }
}
