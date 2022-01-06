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
#[macro_use]
extern crate lazy_static;

mod util;
use crate::util::estree_expression::{Literal, LiteralValue};
use util::estree_expression::ESTreeExpression;

#[test]
fn test_parsing() {
    let v = ESTreeExpression::Literal(Literal {
        value: LiteralValue::Number(23.3),
        raw: "23.30".to_string(),
    });
    let s = serde_json::to_string(&v).unwrap();
    println!("s: {}", s);
    // let v = serde_json::from_str("{}")
    // println!("Here");

    let d: ESTreeExpression =
        serde_json::from_str(r#"{"type":"Literal","value":23.3,"raw":"23.30"}"#).unwrap();
    println!("d: {:?}", d);
}

#[test]
fn test_to_proto() {
    let d: ESTreeExpression =
        serde_json::from_str(r#"{"type":"Literal","value":23.3,"raw":"23.30"}"#).unwrap();
    println!("d: {:?}", d);

    let proto_expr = d.to_proto();
    println!("proto_expr: {:?}", proto_expr);
    println!("proto_expr: {}", proto_expr);

    println!("equal: {}", proto_expr == proto_expr);
}
