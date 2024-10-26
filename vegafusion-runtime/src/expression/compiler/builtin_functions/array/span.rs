use datafusion_common::DFSchema;
use datafusion_expr::{lit, when, Expr, ExprSchemable};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_functions_nested::length::array_length;
use std::ops::Sub;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};

pub fn span_transform(args: &[Expr], schema: &DFSchema) -> vegafusion_common::error::Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        match dtype {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
                if field.data_type().is_numeric() {
                    let len = array_length(arg.clone()).cast_to(&DataType::Int32, schema)?;
                    let first_el = array_element(arg.clone(), lit(1));
                    let last_el = array_element(arg.clone(), len.clone());
                    Ok(when(len.eq(lit(0)), lit(0.0)).otherwise(last_el.sub(first_el))?)
                } else {
                    Ok(lit(0.0))
                }
            }
            _ => {
                // Span of non-array is zero
                Ok(lit(0.0))
            }
        }
    } else {
        Err(VegaFusionError::parse(format!(
            "span requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
