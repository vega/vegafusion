use crate::expression::compiler::utils::to_boolean;
use datafusion_expr::Expr;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::{Result, VegaFusionError};

pub fn to_boolean_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        to_boolean(arg, schema)
    } else {
        Err(VegaFusionError::parse(format!(
            "toBoolean requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
