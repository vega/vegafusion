use datafusion_common::DFSchema;
use datafusion_expr::{Expr, ExprSchemable};

use datafusion_functions::unicode::expr_fn::character_length;
use datafusion_functions_nested::length::array_length;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};

pub fn length_transform(
    args: &[Expr],
    schema: &DFSchema,
) -> vegafusion_common::error::Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        let len_expr = match dtype {
            DataType::Utf8 | DataType::LargeUtf8 => {
                Ok(character_length(arg))
            },
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                Ok(array_length(arg))
            },
            _ => Err(VegaFusionError::parse(format!(
                "length function support array and string arguments. Received argument with type {:?}",
                dtype
            ))),
        }?;

        Ok(len_expr.cast_to(&DataType::Float64, schema)?)
    } else {
        Err(VegaFusionError::parse(format!(
            "length requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
