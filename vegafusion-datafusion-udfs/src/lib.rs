#[macro_use]
extern crate lazy_static;

pub mod udafs;
pub mod udfs;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
