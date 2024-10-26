use datafusion_expr::{in_list, lit, Expr, ExprSchemable};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
pub fn is_finite_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        Ok(match dtype {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => in_list(
                arg,
                vec![lit(f32::NAN), lit(f32::INFINITY), lit(f32::NEG_INFINITY)],
                true,
            ),
            _ => {
                // Non-float types cannot be non-finite
                lit(true)
            }
        })
    } else {
        Err(VegaFusionError::parse(format!(
            "isValid requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
