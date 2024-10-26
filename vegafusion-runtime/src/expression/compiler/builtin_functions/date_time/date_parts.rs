use crate::expression::compiler::call::TzTransformFn;
use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::timeunit::to_timestamp_col;
use datafusion_expr::{lit, Expr};
use datafusion_functions::expr_fn::{date_part, floor};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;

pub fn make_local_datepart_transform(part: &str, tx: Option<fn(Expr) -> Expr>) -> TzTransformFn {
    let part = part.to_string();
    let local_datepart_transform =
        move |tz_config: &RuntimeTzConfig, args: &[Expr], schema: &DFSchema| -> Result<Expr> {
            let arg = args.first().unwrap().clone();
            let arg = to_timestamp_col(arg, schema, &tz_config.default_input_tz.to_string())?;
            let mut expr = date_part(
                lit(part.clone()),
                arg.try_cast_to(
                    &DataType::Timestamp(
                        TimeUnit::Millisecond,
                        Some(tz_config.local_tz.to_string().into()),
                    ),
                    schema,
                )?,
            );

            if let Some(tx) = tx {
                expr = tx(expr)
            }

            Ok(expr)
        };
    Arc::new(local_datepart_transform)
}

pub fn make_utc_datepart_transform(part: &str, tx: Option<fn(Expr) -> Expr>) -> TzTransformFn {
    let part = part.to_string();
    let utc_datepart_transform =
        move |tz_config: &RuntimeTzConfig, args: &[Expr], schema: &DFSchema| -> Result<Expr> {
            let arg = to_timestamp_col(
                args.first().unwrap().clone(),
                schema,
                &tz_config.default_input_tz.to_string(),
            )?;
            let mut expr = date_part(
                lit(part.clone()),
                arg.try_cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    schema,
                )?,
            );

            if let Some(tx) = tx {
                expr = tx(expr)
            }

            Ok(expr)
        };
    Arc::new(utc_datepart_transform)
}

lazy_static! {
    // Local Transforms
    pub static ref YEAR_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("year", None);
    pub static ref QUARTER_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("quarter", None);

    // Months are zero-based in Vega
    pub static ref MONTH_TRANSFORM: TzTransformFn =
        make_local_datepart_transform(
            "month", Some(|expr| expr - lit(1.0))
        );

    pub static ref DAYOFYEAR_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("doy", None);
    pub static ref DATE_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("day", None);
    pub static ref DAY_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("dow", None);
    pub static ref HOUR_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("hour", None);
    pub static ref MINUTE_TRANSFORM: TzTransformFn =
        make_local_datepart_transform("minute", None);
    pub static ref SECOND_TRANSFORM: TzTransformFn =
        make_local_datepart_transform(
            "second", Some(floor)
        );
    pub static ref MILLISECOND_TRANSFORM: TzTransformFn =
        make_local_datepart_transform(
            "millisecond",  Some(|expr| expr % lit(1000.0))
        );

    // UTC Transforms
    pub static ref UTCYEAR_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("year", None);
    pub static ref UTCQUARTER_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("quarter", None);

    // Months are zero-based in Vega
    pub static ref UTCMONTH_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform(
            "month", Some(|expr| expr - lit(1.0))
        );

    pub static ref UTCDAYOFYEAR_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("doy", None);
    pub static ref UTCDATE_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("day", None);
    pub static ref UTCDAY_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("dow", None);
    pub static ref UTCHOUR_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("hour", None);
    pub static ref UTCMINUTE_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform("minute", None);
    pub static ref UTCSECOND_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform(
            "second", Some(floor)
        );
    pub static ref UTCMILLISECOND_TRANSFORM: TzTransformFn =
        make_utc_datepart_transform(
            "millisecond", Some(|expr| expr % lit(1000.0))
        );
}
