use datafusion_expr::Expr;
use vegafusion_common::datafusion_common::DFSchema;

use vegafusion_core::error::{Result, VegaFusionError};

/// `isValid(value)`
///
/// Returns true if value is not null, undefined, or NaN, false otherwise.
///
/// Note: Current implementation does not consider NaN values invalid
///
/// See: https://vega.github.io/vega/docs/expressions/#isValid
pub fn is_valid_fn(args: &[Expr], _schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        Ok(Expr::IsNotNull(Box::new(arg)))
    } else {
        Err(VegaFusionError::parse(format!(
            "isValid requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
