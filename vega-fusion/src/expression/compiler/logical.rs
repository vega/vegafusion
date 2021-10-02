use crate::error::Result;
use crate::expression::ast::logical::{LogicalExpression, LogicalOperator};
use crate::expression::compiler::utils::{data_type, to_boolean};
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan::{DFSchema, Expr, Operator};

pub fn compile_logical(
    node: &LogicalExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Compile branches
    let compiled_lhs = compile(&node.left, config, Some(schema))?;
    let compiled_rhs = compile(&node.right, config, Some(schema))?;

    let lhs_dtype = data_type(&compiled_lhs, schema)?;
    let rhs_dtype = data_type(&compiled_lhs, schema)?;

    let new_expr = match (lhs_dtype, rhs_dtype) {
        (DataType::Boolean, DataType::Boolean) => {
            // If both are boolean, the use regular logical operation
            match node.operator {
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

            match node.operator {
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
