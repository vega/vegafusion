#[macro_use]
extern crate lazy_static;
extern crate core;

pub mod data;
pub mod expression;
pub mod patch;
pub mod planning;
pub mod proto;
pub mod spec;
pub mod task_graph;
pub mod transform;
pub mod variable;

pub use vegafusion_common::arrow;
pub use vegafusion_common::error;

#[cfg(test)]
mod tests {
    // use crate::{create_large_shirt, serialize_shirt, deserialize_shirt};
    use crate::proto::gen::expression;
    use prost::Message;
    use std::io::Cursor;

    #[test]
    fn try_it() {
        let lit = expression::Literal {
            raw: "23.5000".to_string(),
            value: Some(expression::literal::Value::Number(23.5)),
        };

        let mut buf = Vec::new();
        buf.reserve(lit.encoded_len());
        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        lit.encode(&mut buf).unwrap();

        println!("literal: {lit:?}");
        println!("encoded: {buf:?}");

        let decoded = expression::Literal::decode(&mut Cursor::new(&buf));
        println!("decoded: {decoded:?}");
    }
}
