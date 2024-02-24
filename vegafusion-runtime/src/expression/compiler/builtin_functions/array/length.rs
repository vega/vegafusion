use datafusion_common::DFSchema;
use datafusion_expr::{expr, BuiltinScalarFunction, Expr, ExprSchemable, ScalarFunctionDefinition};
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
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Expr::Cast(expr::Cast {
                expr: Box::new(Expr::ScalarFunction(expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::CharacterLength),
                    args: vec![arg],
                })),
                data_type: DataType::Float64
            })),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Ok(Expr::Cast(expr::Cast {
                expr: Box::new(Expr::ScalarFunction(expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayLength),
                    args: vec![arg],
                })),
                data_type: DataType::Float64
            })),
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
