use crate::expression::compiler::utils::to_boolean;
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::logical_plan::{DFSchema, Expr};
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ConditionalExpression;

pub fn compile_conditional(
    node: &ConditionalExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Compile branches
    let compiled_test = compile(node.test(), config, Some(schema))?;
    let compiled_consequent = compile(node.consequent(), config, Some(schema))?;
    let compiled_alternate = compile(node.alternate(), config, Some(schema))?;

    let test = to_boolean(compiled_test, schema)?;

    Ok(Expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(test), Box::new(compiled_consequent))],
        else_expr: Some(Box::new(compiled_alternate)),
    })
}
