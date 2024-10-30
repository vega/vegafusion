use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::timeunit::to_timestamp_col;
use datafusion_expr::{lit, Expr};
use datafusion_functions::expr_fn::to_char;
use std::collections::HashMap;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_core::arrow::datatypes::TimeUnit;
use vegafusion_core::error::{Result, VegaFusionError};

pub fn time_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let format_str = d3_to_chrono_format(&extract_format_str(args)?);

    // Handle format timezone override
    let format_tz_str = if args.len() >= 3 {
        // Second argument is an override local timezone string
        let format_tz_expr = &args[2];
        if let Expr::Literal(ScalarValue::Utf8(Some(format_tz_str))) = format_tz_expr {
            format_tz_str.clone()
        } else {
            return Err(VegaFusionError::parse(
                "Third argument to timeFormat must be a timezone string",
            ));
        }
    } else {
        tz_config.local_tz.to_string()
    };

    let timestamptz_expr = to_timestamp_col(
        args[0].clone(),
        schema,
        &tz_config.default_input_tz.to_string(),
    )?
    .try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some(format_tz_str.into())),
        schema,
    )?;

    Ok(to_char(timestamptz_expr, lit(format_str)))
}

pub fn utc_format_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> Result<Expr> {
    let format_str = d3_to_chrono_format(&extract_format_str(args)?);
    let timestamptz_expr = to_timestamp_col(
        args[0].clone(),
        schema,
        &tz_config.default_input_tz.to_string(),
    )?
    .try_cast_to(
        &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        schema,
    )?;

    Ok(to_char(timestamptz_expr, lit(format_str)))
}

pub fn extract_format_str(args: &[Expr]) -> Result<String> {
    let format_str = if args.len() >= 2 {
        let format_arg = &args[1];
        match format_arg {
            Expr::Literal(ScalarValue::Utf8(Some(format_str))) => Ok(format_str.clone()),
            _ => {
                return Err(VegaFusionError::compilation(
                    "the second argument to the timeFormat function must be a literal string",
                ))
            }
        }
    } else if args.len() == 1 {
        Ok("%I:%M".to_string())
    } else {
        Err(VegaFusionError::compilation(
            "the timeFormat function must have at least one argument",
        ))
    }?;
    Ok(format_str)
}

pub fn d3_to_chrono_format(format: &str) -> String {
    // Initialize mapping of special cases
    let mut special_cases = HashMap::new();
    special_cases.insert("%L", "%3f"); // D3 milliseconds to Chrono's 3-digit fractional seconds
    special_cases.insert("%f", "%6f"); // D3 microseconds to Chrono's 6-digit fractional seconds
    special_cases.insert("%Q", ""); // D3 milliseconds since epoch not supported
    special_cases.insert("%q", ""); // Quarter not directly supported in Chrono
    special_cases.insert("%Z", "%:z"); // D3's %Z is similar to Chrono's %:z (offset without colon)

    let mut result = String::new();
    let mut chars = format.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            if let Some(&next_char) = chars.peek() {
                let specifier = format!(r"%{next_char}");
                if let Some(replacement) = special_cases.get(specifier.as_str()) {
                    result.push_str(replacement);
                } else {
                    result.push_str(&specifier);
                }
                chars.next();
            }
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_conversion() {
        assert_eq!(d3_to_chrono_format("%Y-%m-%d"), "%Y-%m-%d");
        assert_eq!(d3_to_chrono_format("%H:%M:%S"), "%H:%M:%S");
        assert_eq!(d3_to_chrono_format("%%"), "%%");
    }

    #[test]
    fn test_special_cases() {
        assert_eq!(d3_to_chrono_format("%L"), "%3f");
        assert_eq!(d3_to_chrono_format("%f"), "%6f");
        assert_eq!(d3_to_chrono_format("%Z"), "%:z");
        assert_eq!(d3_to_chrono_format("%Q"), "");
        assert_eq!(d3_to_chrono_format("%q"), "");
    }

    #[test]
    fn test_complex_format() {
        assert_eq!(
            d3_to_chrono_format("%Y-%m-%d %H:%M:%S.%L %Z"),
            "%Y-%m-%d %H:%M:%S.%3f %:z"
        );
    }
}
