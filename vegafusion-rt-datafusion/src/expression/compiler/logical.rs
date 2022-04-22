/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::utils::{cast_to, data_type, is_numeric_datatype, to_boolean};
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr, Operator};
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::{LogicalExpression, LogicalOperator};

pub fn compile_logical(
    node: &LogicalExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Compile branches
    let mut compiled_lhs = compile(node.left(), config, Some(schema))?;
    let mut compiled_rhs = compile(node.right(), config, Some(schema))?;

    let lhs_dtype = data_type(&compiled_lhs, schema)?;
    let rhs_dtype = data_type(&compiled_rhs, schema)?;

    let new_expr = match (&lhs_dtype, &rhs_dtype) {
        (DataType::Boolean, DataType::Boolean) => {
            // If both are boolean, the use regular logical operation
            match node.to_operator() {
                LogicalOperator::Or => Expr::BinaryExpr {
                    left: Box::new(compiled_lhs),
                    op: Operator::Or,
                    right: Box::new(compiled_rhs),
                },
                LogicalOperator::And => Expr::BinaryExpr {
                    left: Box::new(compiled_lhs),
                    op: Operator::And,
                    right: Box::new(compiled_rhs),
                },
            }
        }
        _ => {
            // Not both boolean, compile to CASE expression so the results will be drawn
            // from the LHS and RHS
            let lhs_boolean = to_boolean(compiled_lhs.clone(), schema)?;

            // If one side boolean and the other numeric, cast the boolean column to match the
            // numeric one since DataFusion doesn't allow this automatically (for good reason!)
            if is_numeric_datatype(&lhs_dtype) && rhs_dtype == DataType::Boolean {
                compiled_rhs = cast_to(compiled_rhs, &lhs_dtype, schema)?;
            } else if is_numeric_datatype(&rhs_dtype) && lhs_dtype == DataType::Boolean {
                compiled_lhs = cast_to(compiled_lhs, &rhs_dtype, schema)?;
            }

            match node.to_operator() {
                LogicalOperator::Or => Expr::Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(lhs_boolean), Box::new(compiled_lhs))],
                    else_expr: Some(Box::new(compiled_rhs)),
                },
                LogicalOperator::And => Expr::Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(lhs_boolean), Box::new(compiled_rhs))],
                    else_expr: Some(Box::new(compiled_lhs)),
                },
            }
        }
    };

    Ok(new_expr)
}
