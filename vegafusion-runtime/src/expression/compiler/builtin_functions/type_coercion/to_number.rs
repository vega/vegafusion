use datafusion_expr::{Expr, ExprSchemable};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

pub fn to_number_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        if !is_numeric_datatype(&dtype) {
            cast_to(arg, &DataType::Float64, schema)
        } else {
            Ok(arg)
        }
    } else {
        Err(VegaFusionError::parse(format!(
            "toNumber requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
