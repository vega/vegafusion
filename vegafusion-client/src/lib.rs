#[macro_use]
extern crate lazy_static;

pub mod expression;

#[cfg(test)]
mod tests {
    use crate::expression::lexer::tokenize;

    #[test]
    fn it_works() {
        let tokens = tokenize("2 + 3").unwrap();
        println!("tokens: {:?}", tokens);
        assert_eq!(2 + 2, 4);
    }
}

