/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::utils::cast_to;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr};
use vegafusion_core::error::{Result, VegaFusionError};

pub fn to_boolean_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.len() == 1 {
        let arg = args[0].clone();
        cast_to(arg, &DataType::Boolean, schema)
    } else {
        Err(VegaFusionError::parse(format!(
            "toBoolean requires a single argument. Received {} arguments",
            args.len()
        )))
    }
}
