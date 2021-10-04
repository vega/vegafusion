#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod expression;
pub mod transform;
pub mod variable;
pub mod runtime;
pub mod data;
pub mod spec;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
