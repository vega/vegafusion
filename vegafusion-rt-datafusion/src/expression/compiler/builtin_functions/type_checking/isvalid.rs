/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::is_not_null;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;

/// `isValid(value)`
///
/// Returns true if value is not null, undefined, or NaN, false otherwise.
///
/// Note: Current implementation does not consider NaN values invalid
///
/// See: https://vega.github.io/vega/docs/expressions/#isValid
pub fn make_is_valid_udf() -> ScalarUDF {
    let is_valid = |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        let result = is_not_null(arg.as_ref()).unwrap();
        Ok(Arc::new(result) as ArrayRef)
    };
    let is_valid = make_scalar_function(is_valid);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));
    ScalarUDF::new(
        "isValid",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &is_valid,
    )
}
