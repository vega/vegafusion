use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion_expr::Expr;
use datafusion_functions_nested::expr_fn::make_array;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ArrayExpression;

pub fn compile_array(
    node: &ArrayExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let mut elements: Vec<Expr> = Vec::new();
    for el in &node.elements {
        let phys_expr = compile(el, config, Some(schema))?;
        elements.push(phys_expr);
    }
    Ok(make_array(elements))
}
