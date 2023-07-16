use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::{coalesce, concat, lit, Expr, Operator};
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{
    cast_to, data_type, is_null_literal, is_numeric_datatype, is_string_datatype, to_numeric,
    to_string,
};

use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::{BinaryExpression, BinaryOperator};

pub fn compile_binary(
    node: &BinaryExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // First, compile argument
    let lhs = compile(node.left(), config, Some(schema))?;
    let rhs = compile(node.right(), config, Some(schema))?;

    let lhs_dtype = data_type(&lhs, schema)?;
    let rhs_dtype = data_type(&rhs, schema)?;
    let lhs_numeric = to_numeric(lhs.clone(), schema)?;
    let rhs_numeric = to_numeric(rhs.clone(), schema)?;

    let new_expr: Expr = match node.to_operator() {
        BinaryOperator::Minus => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Minus,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::Mult => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Multiply,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::Div => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(cast_to(lhs_numeric, &DataType::Float64, schema)?),
            op: Operator::Divide,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::Mod => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Modulo,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::LessThan => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Lt,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::LessThanEqual => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::LtEq,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::GreaterThan => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::Gt,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::GreaterThanEqual => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lhs_numeric),
            op: Operator::GtEq,
            right: Box::new(rhs_numeric),
        }),
        BinaryOperator::StrictEquals => {
            // Use original values, not those converted to numeric
            // Let DataFusion handle numeric casting
            if is_null_literal(&lhs) {
                Expr::IsNull(Box::new(rhs))
            } else if is_null_literal(&rhs) {
                Expr::IsNull(Box::new(lhs))
            } else if is_numeric_datatype(&lhs_dtype) && is_numeric_datatype(&rhs_dtype)
                || lhs_dtype == rhs_dtype
            {
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::Eq,
                    right: Box::new(rhs),
                })
            } else {
                // Types are not compatible
                lit(false)
            }
        }
        BinaryOperator::NotStrictEquals => {
            if is_null_literal(&lhs) {
                Expr::IsNotNull(Box::new(rhs))
            } else if is_null_literal(&rhs) {
                Expr::IsNotNull(Box::new(lhs))
            } else if is_numeric_datatype(&lhs_dtype) && is_numeric_datatype(&rhs_dtype)
                || lhs_dtype == rhs_dtype
            {
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::NotEq,
                    right: Box::new(rhs),
                })
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
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::Plus,
                    right: Box::new(rhs_numeric),
                })
            }
        }
        BinaryOperator::Equals => {
            if is_null_literal(&lhs) {
                Expr::IsNull(Box::new(rhs))
            } else if is_null_literal(&rhs) {
                Expr::IsNull(Box::new(lhs))
            } else if is_string_datatype(&lhs_dtype) && is_string_datatype(&rhs_dtype) {
                // Regular equality on strings
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::Eq,
                    right: Box::new(rhs),
                })
            } else {
                // Both sides converted to numbers
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::Eq,
                    right: Box::new(rhs_numeric),
                })
            }
            // TODO: if both null, then equal. If one null, then not equal
        }
        BinaryOperator::NotEquals => {
            if is_null_literal(&lhs) {
                Expr::IsNotNull(Box::new(rhs))
            } else if is_null_literal(&rhs) {
                Expr::IsNotNull(Box::new(lhs))
            } else if is_string_datatype(&lhs_dtype) && is_string_datatype(&rhs_dtype) {
                // Regular inequality on strings
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs),
                    op: Operator::NotEq,
                    right: Box::new(rhs),
                })
            } else {
                // Both sides converted to numbers
                // Both sides converted to numbers
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs_numeric),
                    op: Operator::NotEq,
                    right: Box::new(rhs_numeric),
                })
            }
            // TODO: if both null, then equal. If one null, then not equal
        }
        BinaryOperator::BitwiseAnd => bitwise_expr(lhs, Operator::BitwiseAnd, rhs, schema)?,
        BinaryOperator::BitwiseOr => bitwise_expr(lhs, Operator::BitwiseOr, rhs, schema)?,
        BinaryOperator::BitwiseXor => bitwise_expr(lhs, Operator::BitwiseXor, rhs, schema)?,
        BinaryOperator::BitwiseShiftLeft => {
            bitwise_expr(lhs, Operator::BitwiseShiftLeft, rhs, schema)?
        }
        BinaryOperator::BitwiseShiftRight => {
            bitwise_expr(lhs, Operator::BitwiseShiftRight, rhs, schema)?
        }
    };

    Ok(new_expr)
}

fn bitwise_expr(lhs: Expr, op: Operator, rhs: Expr, schema: &DFSchema) -> Result<Expr> {
    // Vega treats null as zero for bitwise operations
    let left = coalesce(vec![cast_to(lhs, &DataType::Int64, schema)?, lit(0)]);
    let right = coalesce(vec![cast_to(rhs, &DataType::Int64, schema)?, lit(0)]);

    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }))
}
