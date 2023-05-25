use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion_expr::{expr, Expr};
use std::ops::Deref;
use std::sync::Arc;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ArrayExpression;
use vegafusion_datafusion_udfs::udfs::array::constructor::ARRAY_CONSTRUCTOR_UDF;

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
    Ok(Expr::ScalarUDF(expr::ScalarUDF {
        fun: Arc::new(ARRAY_CONSTRUCTOR_UDF.deref().clone()),
        args: elements,
    }))
}
