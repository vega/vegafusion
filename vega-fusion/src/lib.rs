#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod expression;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
