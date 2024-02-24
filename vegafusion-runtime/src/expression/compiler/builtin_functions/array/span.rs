use std::ops::Sub;
use datafusion_common::DFSchema;
use datafusion_expr::{BuiltinScalarFunction, Expr, expr, ExprSchemable, lit, ScalarFunctionDefinition, when};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::error::{ResultWithContext, VegaFusionError};

// Note: I believe this implementation of span, using built-in DataFusion functions, is correct.
// But the DataFusion simplifier doesn't seem to know how to simplify it, which is what we use for
// scalar evaluation, so we can't use it yet.
pub fn span_transform(args: &[Expr], schema: &DFSchema) -> vegafusion_common::error::Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        match dtype {
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
                if field.data_type().is_numeric() {
                    let len = Expr::ScalarFunction(expr::ScalarFunction {
                        func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayLength),
                        args: vec![arg.clone()],
                    }).cast_to(&DataType::Int32, schema)?;

                    let first_el = Expr::ScalarFunction(expr::ScalarFunction {
                        func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayElement),
                        args: vec![arg.clone(), lit(1)],
                    });

                    let last_el = Expr::ScalarFunction(expr::ScalarFunction {
                        func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayElement),
                        args: vec![arg.clone(), len.clone()],
                    });

                    Ok(when(len.eq(lit(0)), lit(0.0)).otherwise(last_el.sub(first_el))?)
                } else {
                    Ok(lit(0.0))
                }
            },
            _ => {
                // Span of non-array is zero
                Ok(lit(0.0))
            },
        }
    } else {
        Err(VegaFusionError::parse(format!(
            "span requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}