/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::builtin_functions::date_time::epoch_to_timestamptz::EPOCH_MS_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::str_to_timestamptz::STR_TO_TIMESTAMPTZ_UDF;
use crate::expression::compiler::builtin_functions::date_time::timestamptz_to_timestamp::TIMESTAMPTZ_TO_TIMESTAMP_UDF;
use crate::expression::compiler::call::{TransformFn, TzTransformFn};
use crate::expression::compiler::utils::{cast_to, is_numeric_datatype, VfSimplifyInfo};
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr, ExprSimplifiable};
use datafusion_expr::{lit, BuiltinScalarFunction, ExprSchemable};
use std::sync::Arc;
use vegafusion_core::error::{Result, VegaFusionError};

pub fn make_local_datepart_transform(part: &str, offset: Option<i32>) -> TzTransformFn {
    let part = part.to_string();
    let local_datepart_transform = move |tz_config: &RuntimeTzConfig,
                                         args: &[Expr],
                                         schema: &DFSchema|
          -> Result<Expr> {
        let arg =
            extract_timestamp_arg(&part, args, schema, &tz_config.default_input_tz.to_string())?;
        let udf_args = vec![arg, lit(tz_config.local_tz.to_string())];
        let timestamp = Expr::ScalarUDF {
            fun: Arc::new((*TIMESTAMPTZ_TO_TIMESTAMP_UDF).clone()),
            args: udf_args,
        };

        let mut expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit(part.clone()), timestamp],
        };

        if let Some(offset) = offset {
            expr = expr + lit(offset);
        }

        println!("expr: {}", expr);

        Ok(expr)
    };
    Arc::new(local_datepart_transform)
}

pub fn make_utc_datepart_transform(part: &str, offset: Option<i32>) -> TzTransformFn {
    let part = part.to_string();
    let utc_datepart_transform = move |tz_config: &RuntimeTzConfig,
                                       args: &[Expr],
                                       schema: &DFSchema|
          -> Result<Expr> {
        let arg =
            extract_timestamp_arg(&part, args, schema, &tz_config.default_input_tz.to_string())?;
        let udf_args = vec![lit(part.clone()), arg];
        let mut expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::DatePart,
            args: udf_args,
        };

        if let Some(offset) = offset {
            expr = expr + lit(offset)
        }

        Ok(expr)
    };
    Arc::new(utc_datepart_transform)
}

fn extract_timestamp_arg(
    part: &str,
    args: &[Expr],
    schema: &DFSchema,
    default_input_tz: &str,
) -> Result<Expr> {
    if let Some(arg) = args.get(0) {
        Ok(match arg.get_type(schema)? {
            DataType::Timestamp(_, _) => arg.clone(),
            DataType::Utf8 => Expr::ScalarUDF {
                fun: Arc::new((*STR_TO_TIMESTAMPTZ_UDF).clone()),
                args: vec![arg.clone(), lit("UTC")],
            },
            dtype if is_numeric_datatype(&dtype) => Expr::ScalarUDF {
                fun: Arc::new((*EPOCH_MS_TO_TIMESTAMPTZ_UDF).clone()),
                args: vec![
                    cast_to(arg.clone(), &DataType::Int64, schema)?,
                    lit(default_input_tz),
                ],
            },
            dtype => {
                return Err(VegaFusionError::compilation(format!(
                    "Invalid data type for {} function: {:?}",
                    part, dtype
                )))
            }
        })
    } else {
        Err(VegaFusionError::compilation(format!(
            "{} expects a single argument, received {}",
            part,
            args.len()
        )))
    }
}

lazy_static! {
    // Local Transforms
    pub static ref YEAR_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("year", None);
    pub static ref MONTH_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("month", Some(-1));
    pub static ref DATE_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("day", None);
    pub static ref HOUR_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("hour", None);
    pub static ref MINUTE_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("minute", None);
    pub static ref SECOND_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("second", None);

    // UTC Transforms
    pub static ref UTCYEAR_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("year", None);
    pub static ref UTCMONTH_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("month", Some(-1));
    pub static ref UTCDATE_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("day", None);
    pub static ref UTCHOUR_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("hour", None);
    pub static ref UTCMINUTE_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("minute", None);
    pub static ref UTCSECOND_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("second", None);
}
