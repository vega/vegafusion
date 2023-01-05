#![allow(dead_code)]

pub mod check;
pub mod datasets;
pub mod equality;
pub mod estree_expression;
pub mod vegajs_runtime;

pub fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}
