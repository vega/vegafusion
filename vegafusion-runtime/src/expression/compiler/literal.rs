use datafusion_expr::{lit, Expr};
use vegafusion_common::datafusion_common::ScalarValue;
use vegafusion_core::proto::gen::expression::{literal, Literal};

pub fn compile_literal(node: &Literal) -> Expr {
    use literal::Value::*;
    let scalar = match node.value() {
        Number(value) => ScalarValue::Float64(Some(*value)),
        String(value) => ScalarValue::Utf8(Some(value.clone())),
        Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Null(_) => {
            // ScalarValue doesn't have a general Null type, each scalar type is nullable
            // use Float64 here, but operations should always check for null inputs and treat
            // them the same regardless of type
            ScalarValue::Float64(None)
        }
    };
    lit(scalar)
}
