use crate::expression::ast::literal::{Literal, LiteralValue};
use datafusion::logical_plan::{lit, Expr};
use datafusion::scalar::ScalarValue;

pub fn compile_literal(node: &Literal) -> Expr {
    let scalar = match &node.value {
        LiteralValue::Number(value) => ScalarValue::Float64(Some(*value)),
        LiteralValue::String(value) => ScalarValue::Utf8(Some(value.clone())),
        LiteralValue::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        LiteralValue::Null => {
            // ScalarValue doesn't have a general Null type, each scalar type is nullable
            // use Float64 here, but operations should always check for null inputs and treat
            // them the same regardless of type
            ScalarValue::Float64(None)
        }
    };
    lit(scalar)
}
