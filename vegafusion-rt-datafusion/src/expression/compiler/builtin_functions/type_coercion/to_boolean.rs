use crate::expression::compiler::utils::to_boolean;
use datafusion::common::DFSchema;
use datafusion::logical_expr::Expr;
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
