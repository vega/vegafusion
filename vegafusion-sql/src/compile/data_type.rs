use crate::ast::data_type::DataType as SqlDataType;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait ToSqlDataType {
    fn to_sql(&self) -> Result<SqlDataType>;
}

impl ToSqlDataType for DataType {
    fn to_sql(&self) -> Result<SqlDataType> {
        match self {
            DataType::Null => {
                // No Null available here
                Ok(SqlDataType::Real)
            }
            DataType::Boolean => Ok(SqlDataType::Boolean),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(SqlDataType::Int(None))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(SqlDataType::UnsignedInt(None))
            }
            DataType::Float16 => Ok(SqlDataType::Float(Some(16))),
            DataType::Float32 => Ok(SqlDataType::Float(Some(32))),
            DataType::Float64 => Ok(SqlDataType::Float(Some(64))),
            DataType::Timestamp(_, _) => Err(VegaFusionError::internal(
                "Timestamp cannot be converted to SQL",
            )),
            DataType::Date32 => Err(VegaFusionError::internal(
                "Date32 cannot be converted to SQL",
            )),
            DataType::Date64 => Err(VegaFusionError::internal(
                "Date64 cannot be converted to SQL",
            )),
            DataType::Time32(_) => Err(VegaFusionError::internal(
                "Time32 cannot be converted to SQL",
            )),
            DataType::Time64(_) => Err(VegaFusionError::internal(
                "Time64 cannot be converted to SQL",
            )),
            DataType::Duration(_) => Err(VegaFusionError::internal(
                "Duration cannot be converted to SQL",
            )),
            DataType::Interval(_) => Err(VegaFusionError::internal(
                "Interval cannot be converted to SQL",
            )),
            DataType::Binary => Err(VegaFusionError::internal(
                "Binary cannot be converted to SQL",
            )),
            DataType::FixedSizeBinary(_) => Err(VegaFusionError::internal(
                "FixedSizeBinary cannot be converted to SQL",
            )),
            DataType::LargeBinary => Err(VegaFusionError::internal(
                "LargeBinary cannot be converted to SQL",
            )),
            DataType::Utf8 => Ok(SqlDataType::String),
            DataType::LargeUtf8 => Ok(SqlDataType::String),
            DataType::List(_) => Err(VegaFusionError::internal("List cannot be converted to SQL")),
            DataType::FixedSizeList(_, _) => Err(VegaFusionError::internal(
                "FixedSizeList cannot be converted to SQL",
            )),
            DataType::LargeList(_) => Err(VegaFusionError::internal(
                "LargeList cannot be converted to SQL",
            )),
            DataType::Struct(_) => Err(VegaFusionError::internal(
                "Struct cannot be converted to SQL",
            )),
            DataType::Union(_, _) => Err(VegaFusionError::internal(
                "Union cannot be converted to SQL",
            )),
            DataType::Dictionary(_, _) => Err(VegaFusionError::internal(
                "Dictionary cannot be converted to SQL",
            )),
            DataType::Decimal(_, _) => Err(VegaFusionError::internal(
                "Decimal cannot be converted to SQL",
            )),
            DataType::Map(_, _) => Err(VegaFusionError::internal("Map cannot be converted to SQL")),
        }
    }
}
