/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::logical_plan::{DFSchema, Expr, ExprSchemable};

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
        let arg = args[0].clone();
        let dtype = arg
            .get_type(schema)
            .with_context(|| format!("Failed to infer type of expression: {:?}", arg))?;

        Ok(match dtype {
            DataType::Timestamp(_, _) => lit(true),
            DataType::Date32 => lit(true),
            DataType::Date64 => lit(true),
            _ => lit(false),
        })
    } else {
        Err(VegaFusionError::parse(format!(
            "isDate requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
