use crate::expression::compiler::{compile, config::CompilationConfig};

use datafusion_expr::{lit, Expr};
use datafusion_functions::expr_fn::named_struct;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ObjectExpression;

pub fn compile_object(
    node: &ObjectExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let mut named_struct_args = Vec::new();
    for prop in &node.properties {
        let name = prop.key().to_object_key_string();
        let value_expr = compile(prop.value(), config, Some(schema))?;
        named_struct_args.push(lit(name));
        named_struct_args.push(value_expr);
    }

    Ok(named_struct(named_struct_args))
}
