pub mod table;
pub mod scalar;

#[cfg(not(feature = "datafusion"))]
mod _scalar;

pub mod tasks;
