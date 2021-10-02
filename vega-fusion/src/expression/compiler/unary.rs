use crate::error::Result;
use crate::expression::ast::unary::{UnaryExpression, UnaryOperator};
use crate::expression::compiler::utils::{to_boolean, to_numeric};
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::logical_plan::{DFSchema, Expr};

pub fn compile_unary(
    node: &UnaryExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // First, compile argument
    let argument = compile(&node.argument, config, Some(schema))?;
    let new_expr = match node.operator {
        UnaryOperator::Pos => to_numeric(argument, schema)?,
        UnaryOperator::Neg => Expr::Negative(Box::new(to_numeric(argument, schema)?)),
        UnaryOperator::Not => {
            // Cast to boolean if not already
            Expr::Not(Box::new(to_boolean(argument, schema)?))
        }
    };
    Ok(new_expr)
}
