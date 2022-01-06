/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use datafusion::logical_plan::{DFSchema, Expr};

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
