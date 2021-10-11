#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod proto;
pub mod variable;
pub mod expression;

pub use arrow;

use std::io::Cursor;

use prost::Message;

// Include the `items` module, which is generated from items.proto.
// pub mod expression {
//     include!(concat!(env!("OUT_DIR"), "/vegafusion.expression.rs"));
// }

// pub fn create_large_shirt(color: String) -> items::Shirt {
//     let mut shirt = items::Shirt::default();
//     shirt.color = color;
//     shirt.set_size(items::shirt::Size::Large);
//     shirt
// }
//
// pub fn serialize_shirt(shirt: &items::Shirt) -> Vec<u8> {
//     let mut buf = Vec::new();
//     buf.reserve(shirt.encoded_len());
//     // Unwrap is safe, since we have reserved sufficient capacity in the vector.
//     shirt.encode(&mut buf).unwrap();
//     buf
// }
//
// pub fn deserialize_shirt(buf: &[u8]) -> Result<items::Shirt, prost::DecodeError> {
//     items::Shirt::decode(&mut Cursor::new(buf))
// }


#[cfg(test)]
mod tests {
    // use crate::{create_large_shirt, serialize_shirt, deserialize_shirt};
    use crate::proto::gen::expression;
    use prost::Message;
    use std::io::Cursor;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn try_it() {
        let lit = expression::Literal {
            raw: "23.5000".to_string(),
            value: Some(expression::literal::Value::Number(23.5))
        };

        let mut buf = Vec::new();
        buf.reserve(lit.encoded_len());
        // Unwrap is safe, since we have reserved sufficient capacity in the vector.
        lit.encode(&mut buf).unwrap();

        println!("literal: {:?}", lit);
        println!("encoded: {:?}", buf);

        let decoded = expression::Literal::decode(&mut Cursor::new(&buf));
        println!("decoded: {:?}", decoded);
    }
}
