use datafusion::scalar::ScalarValue;
use sqlgen::ast::Value as SqlValue;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait ToSqlScalar {
    fn to_sql(&self) -> Result<SqlValue>;
}

impl ToSqlScalar for ScalarValue {
    fn to_sql(&self) -> Result<SqlValue> {
        match self {
            ScalarValue::Null => Ok(SqlValue::Null),
            ScalarValue::Boolean(v) => Ok(v.map(SqlValue::Boolean).unwrap_or(SqlValue::Null)),
            ScalarValue::Float32(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Float64(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Int8(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Int16(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Int32(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Int64(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::UInt8(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::UInt16(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::UInt32(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::UInt64(v) => Ok(v
                .map(|v| SqlValue::Number(v.to_string(), false))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Utf8(v) => Ok(v
                .as_ref()
                .map(|v| SqlValue::DoubleQuotedString(v.clone()))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::LargeUtf8(v) => Ok(v
                .as_ref()
                .map(|v| SqlValue::DoubleQuotedString(v.clone()))
                .unwrap_or(SqlValue::Null)),
            ScalarValue::Binary(_) => Err(VegaFusionError::internal(
                "Binary cannot be converted to SQL",
            )),
            ScalarValue::LargeBinary(_) => Err(VegaFusionError::internal(
                "LargeBinary cannot be converted to SQL",
            )),
            ScalarValue::List(_, _) => {
                Err(VegaFusionError::internal("List cannot be converted to SQL"))
            }
            ScalarValue::Date32(_) => Err(VegaFusionError::internal(
                "Date32 cannot be converted to SQL",
            )),
            ScalarValue::Date64(_) => Err(VegaFusionError::internal(
                "Date64 cannot be converted to SQL",
            )),
            ScalarValue::TimestampSecond(_, _) => Err(VegaFusionError::internal(
                "TimestampSecond cannot be converted to SQL",
            )),
            ScalarValue::TimestampMillisecond(_, _) => Err(VegaFusionError::internal(
                "TimestampMillisecond cannot be converted to SQL",
            )),
            ScalarValue::TimestampMicrosecond(_, _) => Err(VegaFusionError::internal(
                "TimestampMicrosecond cannot be converted to SQL",
            )),
            ScalarValue::TimestampNanosecond(_, _) => Err(VegaFusionError::internal(
                "TimestampNanosecond cannot be converted to SQL",
            )),
            ScalarValue::IntervalYearMonth(_) => Err(VegaFusionError::internal(
                "IntervalYearMonth cannot be converted to SQL",
            )),
            ScalarValue::IntervalDayTime(_) => Err(VegaFusionError::internal(
                "IntervalDayTime cannot be converted to SQL",
            )),
            ScalarValue::IntervalMonthDayNano(_) => Err(VegaFusionError::internal(
                "IntervalMonthDayNano cannot be converted to SQL",
            )),
            ScalarValue::Struct(_, _) => Err(VegaFusionError::internal(
                "Struct cannot be converted to SQL",
            )),
            ScalarValue::Dictionary(_, _) => Err(VegaFusionError::internal(
                "Dictionary cannot be converted to SQL",
            )),
            ScalarValue::Decimal128(_, _, _) => Err(VegaFusionError::internal(
                "Decimal128 cannot be converted to SQL",
            )),
        }
    }
}
