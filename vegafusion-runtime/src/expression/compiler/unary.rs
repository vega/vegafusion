use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion_expr::Expr;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{to_boolean, to_numeric};
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::{UnaryExpression, UnaryOperator};

pub fn compile_unary(
    node: &UnaryExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // First, compile argument
    let argument = compile(node.argument(), config, Some(schema))?;
    let new_expr = match node.to_operator() {
        UnaryOperator::Pos => to_numeric(argument, schema)?,
        UnaryOperator::Neg => Expr::Negative(Box::new(to_numeric(argument, schema)?)),
        UnaryOperator::Not => {
            // Cast to boolean if not already
            Expr::Not(Box::new(to_boolean(argument, schema)?))
        }
    };
    Ok(new_expr)
}
