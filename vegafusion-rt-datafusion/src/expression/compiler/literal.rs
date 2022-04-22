/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::logical_plan::{lit, Expr};
use datafusion::scalar::ScalarValue;
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
