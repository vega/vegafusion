/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
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
