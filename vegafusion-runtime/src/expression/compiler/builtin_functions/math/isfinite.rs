use datafusion_expr::{expr, lit, Expr, ExprSchemable, ScalarFunctionDefinition, ScalarUDF};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_datafusion_udfs::udfs::math::isfinite::IsFiniteUDF;

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
pub fn is_finite_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {arg:?}"))?;

        Ok(match dtype {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                Expr::ScalarFunction(expr::ScalarFunction {
                    func_def: ScalarFunctionDefinition::UDF(Arc::new(ScalarUDF::from(
                        IsFiniteUDF::new(),
                    ))),
                    args: vec![arg],
                })
            }
            _ => {
                // Non-float types cannot be non-finite
                lit(true)
            }
        })
    } else {
        Err(VegaFusionError::parse(format!(
            "isValid requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
