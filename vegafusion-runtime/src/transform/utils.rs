use std::ops::{Add, Mul};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::DFSchema;
use datafusion_expr::{case, Expr, ExprSchemable, lit, when};
use datafusion_functions::expr_fn::{date_part, regexp_like, to_timestamp_millis, to_unixtime};
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};
use vegafusion_common::error::{Result, VegaFusionError};
use crate::expression::compiler::utils::ExprHelpers;

pub trait RecordBatchUtils {
    fn equals(&self, other: &RecordBatch) -> bool;
}

impl RecordBatchUtils for RecordBatch {
    fn equals(&self, other: &RecordBatch) -> bool {
        if self.schema() != other.schema() {
            // Schema's are not equal
            return false;
        }

        // Schema's equal, check columns
        let schema = self.schema();

        for (i, _field) in schema.fields().iter().enumerate() {
            let self_array = self.column(i);
            let other_array = other.column(i);
            if self_array != other_array {
                return false;
            }
        }

        true
    }
}

pub fn make_timestamp_parse_formats() -> Vec<Expr> {
    return vec![
        // ISO 8601 with and without time and 'T' separator
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.3f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.3f",
        "%Y-%m-%d %H:%M",
        // With UTC timezone offset
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%.3f%:z",
        "%Y-%m-%dT%H:%M:%S%.3fZ",
        "%Y-%m-%dT%H:%M%:z",
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S%.3f%:z",
        "%Y-%m-%d %H:%M:%S%.3fZ",
        "%Y-%m-%d %H:%M%:z",
        // ISO 8601 with forward slashes
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        // e.g. May 1 2003
        "%b %-d %Y",
        "%b %-d %Y %H:%M:%S",
        "%b %-d %Y %H:%M",
        // ctime format (e.g. Sun Jul 8 00:34:60 2001)
        "%a %b %-d %H:%M:%S %Y",
        "%a %b %-d %H:%M %Y",
        // e.g. 01 Jan 2012 00:00:00
        "%d %b %Y",
        "%d %b %Y %H:%M:%S",
        "%d %b %Y %H:%M",
        // e.g. Sun, 01 Jan 2012 00:00:00
        "%a, %d %b %Y",
        "%a, %d %b %Y %H:%M:%S",
        "%a, %d %b %Y %H:%M",
        // e.g. December 17, 1995 03:00:00
        "%B %d, %Y",
        "%B %d, %Y %H:%M:%S",
        "%B %d, %Y %H:%M",
    ].into_iter().map(lit).collect()
}


/// Build an expression that converts string to timestamps, following the browser's unfortunate
/// convention where ISO8601 dates (not timestamps) are always interpreted as UTC,
/// but all other formats are interpreted as the local timezone.
pub fn str_to_timestamp(s: Expr, default_input_tz: &str, schema: &DFSchema) -> Result<Expr> {
    // Create condition for whether the parsed timestamp (which always starts as naive) should
    // be interpreted as UTC, or as the default_input_tz.
    // There are two cases where we always use UTC:
    //   1. To follow the browser, timestamps of the form 2020-01-01 are always interpreted as UTC
    //   2. Timestamps that have an offset suffix (e.g. '+05:00', '-09:00', or 'Z') are parsed by
    //      datafusion as UTC
    let is_utc_condition = regexp_like(s.clone(), lit(r"^\d{4}-\d{2}-\d{2}$"), None).or(
        regexp_like(s.clone(), lit(r"[+-]\d{2}:\d{2}$"), None)
    ).or(
        regexp_like(s.clone(), lit(r"Z$"), None)
    );

    // Note: it's important for the express to always return values in the same timezone,
    // so we cast the UTC case back to the local timezone
    let if_true = to_timestamp_millis(vec![s.clone()]).try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        schema
    )?.try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
        schema
    )?;

    let if_false = to_timestamp_millis(vec![
        vec![s],
        make_timestamp_parse_formats()
    ].concat()).try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
        schema
    )?;

    let expr = when(is_utc_condition, if_true).otherwise(if_false)?;
    Ok(expr)
}


pub fn to_epoch_millis(expr: Expr, default_input_tz: &str, schema: &DFSchema) -> Result<Expr> {
    // Dispatch handling on data type
    Ok(match expr.get_type(schema)? {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)=> {
            // Interpret as utc milliseconds
            let millis = date_part(lit("millisecond"), expr.clone()).cast_to(&DataType::Int64, schema)?;
            to_unixtime(
                vec![expr.clone()]
            ).mul(lit(1000)).add(millis)
        }
        DataType::Timestamp(_, Some(_)) => {
            // Convert to UTC, then drop timezone
            let millis = date_part(lit("millisecond"), expr.clone()).cast_to(&DataType::Int64, schema)?;
            let expr = expr.try_cast_to(&DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), schema)?
                .try_cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None), schema)?;

            to_unixtime(vec![expr.clone()]).mul(lit(1000)).add(millis)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let millis = date_part(lit("millisecond"), expr.clone()).cast_to(&DataType::Int64, schema)?;
            let expr = str_to_timestamp(expr.clone(), default_input_tz, schema)?
                .try_cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None), schema)?;;
            to_unixtime(vec![expr]).mul(lit(1000)).add(millis)
        }
        DataType::Int64 => {
            // Keep int argument as-is
            expr.clone()
        }
        dtype if is_numeric_datatype(&dtype) || matches!(dtype, DataType::Boolean) => {
            // Cast other numeric types to Int64
            cast_to(expr.clone(), &DataType::Int64, schema)?
        }
        dtype => {
            return Err(VegaFusionError::internal(format!(
                "Invalid argument type to time function: {dtype:?}"
            )))
        }
    })
}