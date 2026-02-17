use datafusion_expr::{lit, Expr, ExprSchemable};
use datafusion_functions::expr_fn::isnan;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

/// `isNaN(value)`
///
/// Returns true only for floating-point NaN values.
///
/// This follows the Vega use-cases in compiled specs while avoiding
/// DataFusion type errors for non-float types (timestamps, integers, etc).
pub fn is_nan_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() != 1 {
        return Err(VegaFusionError::parse(format!(
            "isNaN requires a single argument. Received {} arguments",
            args.len()
        )));
    }

    let arg = args[0].clone();
    let dtype = arg
        .get_type(schema)
        .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

    Ok(match dtype {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => isnan(arg),
        _ => lit(false),
    })
}
