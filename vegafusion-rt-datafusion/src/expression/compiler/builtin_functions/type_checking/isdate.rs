use datafusion::arrow::array::ArrayRef;
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::prelude::lit;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

/// `isDate(value)`
///
/// Returns true if value is a Date object, false otherwise.
/// This method will return false for timestamp numbers or date-formatted strings;
/// it recognizes Date objects only.
///
/// Note: Current implementation does not consider NaN values invalid
///
/// See: https://vega.github.io/vega/docs/expressions/#isDate
pub fn is_date_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        // Datetime from string or integer in milliseconds
        let mut arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

        Ok(match dtype {
            DataType::Timestamp(_, _) => lit(true),
            DataType::Date32 => lit(true),
            DataType::Date64 => lit(true),
            _ => lit(false)
        })
    } else {
        // Numeric date components
        Err(VegaFusionError::parse(
            format!("isDate requires a single argument. Received {} arguments", args.len())
        ))
    }
}