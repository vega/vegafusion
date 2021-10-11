mod utils;

use wasm_bindgen::prelude::*;
use vegafusion_core::proto::gen::expression;
use vegafusion_core::arrow;
use vegafusion_core::arrow::array::Float64Array;
use vegafusion_client::expression::lexer::tokenize;
use vegafusion_client::expression::parser::parse;
use prost::Message;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    let lit = expression::Literal {
        raw: "23.5000".to_string(),
        value: Some(expression::literal::Value::Number(23.5))
    };

    let arr = Float64Array::from(vec![1.0, 2.0, 3.0]);

    let expr = parse("2 + 3").unwrap();
    let mut expr_bytes = Vec::new();
    expr_bytes.reserve(expr.encoded_len());
    // Unwrap is safe, since we have reserved sufficient capacity in the vector.
    expr.encode(&mut expr_bytes).unwrap();

    alert(&format!("Hello, from Rust!\n{:?}\n{:?}\n{:?}\n{:?}", lit, arr, expr, expr_bytes));
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
