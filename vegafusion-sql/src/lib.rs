pub mod compile;
pub mod connection;
pub mod dataframe;
pub mod dialect;

#[macro_use]
extern crate log;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
