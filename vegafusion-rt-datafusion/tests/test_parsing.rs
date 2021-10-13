mod util;
use util::estree_expression::Expression;
use crate::util::estree_expression::{Literal, LiteralValue};

#[test]
fn test_parsing() {
    let v = Expression::Literal(Literal { value: LiteralValue::Number(23.3), raw: "23.30".to_string() });
    let s = serde_json::to_string(&v).unwrap();
    println!("s: {}", s);
    // let v = serde_json::from_str("{}")
    // println!("Here");

    let d: Expression = serde_json::from_str(r#"{"type":"Literal","value":23.3,"raw":"23.30"}"#).unwrap();
    println!("d: {:?}", d);
}

#[test]
fn test_to_proto() {
    let d: Expression = serde_json::from_str(r#"{"type":"Literal","value":23.3,"raw":"23.30"}"#).unwrap();
    println!("d: {:?}", d);

    let proto_expr = d.to_proto();
    println!("proto_expr: {:?}", proto_expr);
    println!("proto_expr: {}", proto_expr);
}
