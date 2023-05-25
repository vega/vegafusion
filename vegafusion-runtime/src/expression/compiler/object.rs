use crate::expression::compiler::{compile, config::CompilationConfig};

use datafusion_expr::{expr, Expr, ExprSchemable};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ObjectExpression;
use vegafusion_datafusion_udfs::udfs::object::make_object_constructor_udf;

pub fn compile_object(
    node: &ObjectExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<Expr> = Vec::new();
    let mut value_types: Vec<DataType> = Vec::new();
    for prop in &node.properties {
        let expr = compile(prop.value(), config, Some(schema))?;
        let name = prop.key().to_object_key_string();
        keys.push(name);
        value_types.push(expr.get_type(schema)?);
        values.push(expr)
    }

    let udf = make_object_constructor_udf(keys.as_slice(), value_types.as_slice());

    Ok(Expr::ScalarUDF(expr::ScalarUDF {
        fun: Arc::new(udf),
        args: values,
    }))
}
