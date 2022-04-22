/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
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
