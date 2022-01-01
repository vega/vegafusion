use crate::expression::compiler::utils::{cast_to, is_numeric_datatype};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

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
