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
use crate::expression::compiler::utils::{cast_to, is_string_datatype, to_boolean};
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::logical_plan::{DFSchema, Expr};
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ConditionalExpression;

pub fn compile_conditional(
    node: &ConditionalExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Compile branches
    let test_expr = compile(node.test(), config, Some(schema))?;
    let consequent_expr = compile(node.consequent(), config, Some(schema))?;
    let alternate_expr = compile(node.alternate(), config, Some(schema))?;

    let test = to_boolean(test_expr, schema)?;

    // DataFusion will mostly handle unifying consequent and alternate expression types. But it
    // won't cast non string types to strings. Do that manually here
    let consequent_dtype = consequent_expr.get_type(schema)?;
    let alternate_dtype = alternate_expr.get_type(schema)?;

    let (consequent_expr, alternate_expr) =
        if is_string_datatype(&consequent_dtype) && !is_string_datatype(&alternate_dtype) {
            (
                consequent_expr,
                cast_to(alternate_expr, &DataType::Utf8, schema)?,
            )
        } else if !is_string_datatype(&consequent_dtype) && is_string_datatype(&alternate_dtype) {
            (
                cast_to(consequent_expr, &DataType::Utf8, schema)?,
                alternate_expr,
            )
        } else {
            (consequent_expr, alternate_expr)
        };

    Ok(Expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(test), Box::new(consequent_expr))],
        else_expr: Some(Box::new(alternate_expr)),
    })
}
