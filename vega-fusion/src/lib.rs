#[macro_use]
extern crate lazy_static;

pub mod data;
pub mod error;
pub mod expression;
pub mod runtime;
pub mod spec;
pub mod transform;
pub mod variable;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
