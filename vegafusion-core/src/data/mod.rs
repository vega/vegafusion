use arrow::datatypes::DataType;

pub mod json_writer;
pub mod scalar;
pub mod table;
pub mod tasks;

const ORDER_COL: &str = "_vf_order";
const ORDER_COL_DTYPE: DataType = DataType::UInt32;