use vegafusion_core::error::Result;
use crate::expression::compiler::utils::{
    data_type, is_numeric_datatype, is_string_datatype, to_numeric, to_string,
};
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::logical_plan::{concat, lit, DFSchema, Expr, Operator};
use vegafusion_core::proto::gen::expression::{BinaryExpression, BinaryOperator};


pub fn compile_binary(
    node: &BinaryExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // First, compile argument
    let lhs = compile(&node.left.as_ref().unwrap(), config, Some(schema))?;
    let rhs = compile(&node.right.as_ref().unwrap(), config, Some(schema))?;

    let lhs_dtype = data_type(&lhs, schema)?;
    let rhs_dtype = data_type(&rhs, schema)?;
    let lhs_numeric = to_numeric(lhs.clone(), schema)?;
    let rhs_numeric = to_numeric(rhs.clone(), schema)?;

    use BinaryOperator::*;
    let new_expr: Expr = match node.to_operator() {
        BinaryOperator::Minus => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Minus,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::Mult => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Multiply,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::Div => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Divide,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::Mod => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Modulo,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::LessThan => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Lt,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::LessThanEqual => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::LtEq,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::GreaterThan => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Gt,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::GreaterThanEqual => Expr::BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::GtEq,
            right: Box::new(rhs_numeric),
        },
        BinaryOperator::StrictEquals => {
            // Use original values, not those converted to numeric
            // Let DataFusion handle numeric casting
            if is_numeric_datatype(&lhs_dtype) && is_numeric_datatype(&rhs_dtype)
                || lhs_dtype == rhs_dtype
            {
                Expr::BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::Eq,
                    right: Box::new(rhs),
                }
            } else {
                // Types are not compatible
                lit(false)
            }
        }
        BinaryOperator::NotStrictEquals => {
            if is_numeric_datatype(&lhs_dtype) && is_numeric_datatype(&rhs_dtype)
                || lhs_dtype == rhs_dtype
            {
                Expr::BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::NotEq,
                    right: Box::new(rhs),
                }
            } else {
                // Types are not compatible
                lit(false)
            }
        }
        BinaryOperator::Plus => {
            if is_string_datatype(&lhs_dtype) || is_string_datatype(&rhs_dtype) {
                // If either argument is a string, then both are treated as string and
                // plus is string concatenation
                let lhs_string = to_string(lhs, schema)?;
                let rhs_string = to_string(rhs, schema)?;
                concat(&[lhs_string, rhs_string])
            } else {
                // Both sides are non-strings, use regular numeric plus operation
                // Use result of to_numeric to handle booleans
                Expr::BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::Plus,
                    right: Box::new(rhs_numeric),
                }
            }
        }
        BinaryOperator::Equals => {
            if is_string_datatype(&lhs_dtype) && is_string_datatype(&rhs_dtype) {
                // Regular equality on strings
                Expr::BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::Eq,
                    right: Box::new(rhs),
                }
            } else {
                // Both sides converted to numbers
                Expr::BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::Eq,
                    right: Box::new(rhs_numeric),
                }
            }
            // TODO: if both null, then equal. If one null, then not equal
        }
        BinaryOperator::NotEquals => {
            if is_string_datatype(&lhs_dtype) && is_string_datatype(&rhs_dtype) {
                // Regular inequality on strings
                Expr::BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::NotEq,
                    right: Box::new(rhs),
                }
            } else {
                // Both sides converted to numbers
                // Both sides converted to numbers
                Expr::BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::NotEq,
                    right: Box::new(rhs_numeric),
                }
            }
            // TODO: if both null, then equal. If one null, then not equal
        }
    };

    Ok(new_expr)
}
