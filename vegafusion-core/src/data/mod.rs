pub mod table;
pub mod url_task;

pub mod scalar;

#[cfg(not(feature = "datafusion"))]
mod _scalar;
