use datafusion_common::ScalarValue;
use datafusion_expr::{binary_expr, lit, when, Expr, ExprSchemable, Operator};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{cast_to, is_integer_datatype, to_numeric};

use vegafusion_core::error::{Result, VegaFusionError};

/// `format(value, specifier)`
///
/// Formats a numeric value as a string. The specifier must be a valid d3-format specifier
/// (e.g., format(value, ',.2f').
///
/// Note: Current implementation only supports empty string as specifier
///
/// See: https://vega.github.io/vega/docs/expressions/#format
pub fn format_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 2 {
        match &args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(s))) if s.is_empty() => {
                let arg = to_numeric(args[0].clone(), schema)?;
                if is_integer_datatype(&arg.get_type(schema)?) {
                    // Integer type, just cast to string
                    cast_to(args[0].clone(), &DataType::Utf8, schema)
                } else {
                    // Float type, need CASE statement so that integer values don't get decimal points
                    Ok(when(
                        binary_expr(arg.clone(), Operator::Modulo, lit(1.0)).eq(lit(0.0)),
                        cast_to(cast_to(arg, &DataType::Int64, schema)?, &DataType::Utf8, schema)?
                    ).otherwise(
                        cast_to(args[0].clone(), &DataType::Utf8, schema)?
                    )?)
                }
            }
            _ => Err(VegaFusionError::parse(format!(
                "format function only supported with empty string as second argument. Reveived {:?}",
                args[1]
            )))
        }
    } else {
        Err(VegaFusionError::parse(format!(
            "format function requires two arguments. Received {} arguments",
            args.len()
        )))
    }
}
