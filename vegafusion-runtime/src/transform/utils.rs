use crate::expression::compiler::builtin_functions::date_time::date_format::d3_to_chrono_format;
use crate::expression::compiler::utils::ExprHelpers;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::DFSchema;
use datafusion_expr::{lit, when, Expr, ExprSchemable};
use datafusion_functions::expr_fn::{make_date, regexp_like, to_timestamp_millis};
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::datatypes::is_numeric_datatype;
use vegafusion_common::error::{Result, VegaFusionError};

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
    vec![
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
        // month/day/year
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
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
    ]
    .into_iter()
    .map(lit)
    .collect()
}

/// Build an expression that converts string to timestamps, following the browser's unfortunate
/// convention where ISO8601 dates (not timestamps) are always interpreted as UTC,
/// but all other formats are interpreted as the local timezone.
pub fn str_to_timestamp(
    s: Expr,
    default_input_tz: &str,
    schema: &DFSchema,
    fmt: Option<&str>,
) -> Result<Expr> {
    if let Some(fmt) = fmt {
        // Parse with single explicit format, in the specified timezone
        let chrono_fmt = d3_to_chrono_format(fmt);

        if chrono_fmt == "%Y" {
            // Chrono won't parse this as years by itself, since it's not technically enough info
            // to make a timestamp, so instead we'll make date on the first of the year
            Ok(make_date(
                s.try_cast_to(&DataType::Int64, schema)?,
                lit(1),
                lit(1),
            ))
        } else {
            // Interpret as utc if the input has a timezone offset
            let is_utc_condition = regexp_like(s.clone(), lit(r"[+-]\d{2}:\d{2}$"), None)
                .or(regexp_like(s.clone(), lit(r"Z$"), None));

            let naive_timestamp = to_timestamp_millis(vec![s, lit(chrono_fmt)]);

            // Interpret as UTC then convert to default_iput
            let if_true = naive_timestamp
                .clone()
                .try_cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    schema,
                )?
                .try_cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
                    schema,
                )?;

            // Interpret as default input
            let if_false = naive_timestamp.try_cast_to(
                &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
                schema,
            )?;

            let expr = when(is_utc_condition, if_true).otherwise(if_false)?;
            Ok(expr)
        }
    } else {
        // Auto formatting;
        // Create condition for whether the parsed timestamp (which always starts as naive) should
        // be interpreted as UTC, or as the default_input_tz.
        // There are two cases where we always use UTC:
        //   1. To follow the browser, timestamps of the form 2020-01-01 are always interpreted as UTC
        //   2. Timestamps that have an offset suffix (e.g. '+05:00', '-09:00', or 'Z') are parsed by
        //      datafusion as UTC
        let is_utc_condition = regexp_like(s.clone(), lit(r"^\d{4}-\d{2}-\d{2}$"), None)
            .or(regexp_like(s.clone(), lit(r"[+-]\d{2}:\d{2}$"), None))
            .or(regexp_like(s.clone(), lit(r"Z$"), None));

        // Note: it's important for the express to always return values in the same timezone,
        // so we cast the UTC case back to the local timezone
        let if_true = to_timestamp_millis(vec![s.clone()])
            .try_cast_to(
                &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                schema,
            )?
            .try_cast_to(
                &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
                schema,
            )?;

        let if_false = to_timestamp_millis([vec![s], make_timestamp_parse_formats()].concat())
            .try_cast_to(
                &DataType::Timestamp(TimeUnit::Millisecond, Some(default_input_tz.into())),
                schema,
            )?;

        let expr = when(is_utc_condition, if_true).otherwise(if_false)?;
        Ok(expr)
    }
}

pub fn from_epoch_millis(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    Ok(expr.try_cast_to(&DataType::Int64, schema)?.try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        schema,
    )?)
}

pub fn to_epoch_millis(expr: Expr, default_input_tz: &str, schema: &DFSchema) -> Result<Expr> {
    // Dispatch handling on data type
    Ok(match expr.get_type(schema)? {
        DataType::Timestamp(TimeUnit::Millisecond, None) | DataType::Date64 => {
            expr.cast_to(&DataType::Int64, schema)?
        }
        DataType::Date32 | DataType::Timestamp(_, None) => expr
            .try_cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None), schema)?
            .cast_to(&DataType::Int64, schema)?,
        DataType::Timestamp(_, Some(_)) => {
            // Convert to UTC, then drop timezone
            expr.try_cast_to(
                &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                schema,
            )?
            .cast_to(&DataType::Int64, schema)?
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            str_to_timestamp(expr.clone(), default_input_tz, schema, None)?
                .try_cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    schema,
                )?
                .cast_to(&DataType::Int64, schema)?
        }
        DataType::Int64 => {
            // Keep int argument as-is
            expr.clone()
        }
        dtype if is_numeric_datatype(&dtype) || matches!(dtype, DataType::Boolean) => {
            // Cast other numeric types to Int64
            expr.clone().try_cast_to(&DataType::Int64, schema)?
        }
        dtype => {
            return Err(VegaFusionError::internal(format!(
                "Invalid argument type to time function: {dtype:?}"
            )))
        }
    })
}
