use datafusion_common::DFSchema;
use datafusion_expr::{lit, Expr, ExprSchemable};
use datafusion_functions::expr_fn::{coalesce, strpos};
use datafusion_functions_nested::expr_fn::array_position;
use std::ops::Sub;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};

pub fn indexof_transform(
    args: &[Expr],
    schema: &DFSchema,
) -> vegafusion_common::error::Result<Expr> {
    if args.len() == 2 {
        let array_expr = args[0].clone();
        let item_expr = args[1].clone();
        let dtype = array_expr
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {array_expr:?}"))?;

        let indexof_expr = match dtype {
            DataType::Utf8 | DataType::LargeUtf8 => {
                Ok(coalesce(vec![
                    strpos(array_expr, item_expr).sub(lit(1)),
                    lit(-1)
                ]))
            },
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                Ok(coalesce(vec![
                    array_position(array_expr, item_expr, lit(1)).sub(lit(1)),
                    lit(-1)
                ]))
            },
            _ => Err(VegaFusionError::parse(format!(
                "indexof function support array and string arguments. Received argument with type {:?}",
                dtype
            ))),
        }?;

        Ok(indexof_expr.cast_to(&DataType::Float64, schema)?)
    } else {
        Err(VegaFusionError::parse(format!(
            "indexof requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
