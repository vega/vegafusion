use datafusion_expr::Expr;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::cast_to;
use vegafusion_core::error::{Result, VegaFusionError};

pub fn to_string_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        cast_to(arg, &DataType::Utf8, schema)
    } else {
        Err(VegaFusionError::parse(format!(
            "toString requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
