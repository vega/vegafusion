pub mod scalar;
pub mod table;

#[cfg(not(feature = "datafusion"))]
mod _scalar;

pub mod tasks;
