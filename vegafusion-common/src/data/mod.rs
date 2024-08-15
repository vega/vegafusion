pub mod table;

#[cfg(feature = "json")]
pub mod json_writer;
pub mod scalar;

use arrow::datatypes::DataType;

pub const ORDER_COL: &str = "_vf_order";
pub const ORDER_COL_DTYPE: DataType = DataType::UInt32;
