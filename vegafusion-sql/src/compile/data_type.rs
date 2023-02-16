use crate::dialect::Dialect;
use arrow::datatypes::DataType;
use sqlparser::ast::DataType as SqlDataType;
use vegafusion_common::error::{Result, VegaFusionError};

pub trait ToSqlDataType {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlDataType>;
}

impl ToSqlDataType for DataType {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlDataType> {
        if let Some(sql_datatype) = dialect.cast_datatypes.get(self) {
            Ok(sql_datatype.clone())
        } else {
            Err(VegaFusionError::sql_not_supported(format!(
                "Data type {self} not supported by dialect"
            )))
        }
    }
}
