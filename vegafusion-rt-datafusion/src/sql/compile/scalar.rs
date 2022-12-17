use datafusion::scalar::ScalarValue;
use sqlgen::ast::{
    Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg, FunctionArgExpr,
    Ident, ObjectName as SqlObjectName, Value as SqlValue,
};
use vegafusion_core::error::{Result, VegaFusionError};

pub trait ToSqlScalar {
    fn to_sql(&self) -> Result<SqlExpr>;
}

impl ToSqlScalar for ScalarValue {
    fn to_sql(&self) -> Result<SqlExpr> {
        match self {
            ScalarValue::Null => Ok(SqlExpr::Value(SqlValue::Null)),
            ScalarValue::Boolean(v) => Ok(SqlExpr::Value(
                v.map(SqlValue::Boolean).unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Float32(v) => Ok(SqlExpr::Value(
                v.map(|v| {
                    let repr = if !v.is_finite() {
                        // Wrap inf, -inf, and nan in single quotes
                        format!("'{}'", v)
                    } else if v.fract() == 0.0 {
                        format!("{:.1}", v)
                    } else {
                        v.to_string()
                    };
                    SqlValue::Number(repr, false)
                })
                .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Float64(v) => Ok(SqlExpr::Value(
                v.map(|v| {
                    let repr = if !v.is_finite() {
                        // Wrap inf, -inf, and nan in single quotes
                        format!("'{}'", v)
                    } else if v.fract() == 0.0 {
                        format!("{:.1}", v)
                    } else {
                        v.to_string()
                    };
                    SqlValue::Number(repr, false)
                })
                .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Int8(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Int16(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Int32(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Int64(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::UInt8(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::UInt16(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::UInt32(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::UInt64(v) => Ok(SqlExpr::Value(
                v.map(|v| SqlValue::Number(v.to_string(), false))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Utf8(v) => Ok(SqlExpr::Value(
                v.as_ref()
                    .map(|v| SqlValue::SingleQuotedString(v.clone()))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::LargeUtf8(v) => Ok(SqlExpr::Value(
                v.as_ref()
                    .map(|v| SqlValue::SingleQuotedString(v.clone()))
                    .unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Binary(_) => Err(VegaFusionError::internal(
                "Binary cannot be converted to SQL",
            )),
            ScalarValue::LargeBinary(_) => Err(VegaFusionError::internal(
                "LargeBinary cannot be converted to SQL",
            )),
            ScalarValue::FixedSizeBinary(_, _) => Err(VegaFusionError::internal(
                "FixedSizeBinary cannot be converted to SQL",
            )),
            ScalarValue::List(args, _) => {
                let function_ident = Ident {
                    value: "make_list".to_string(),
                    quote_style: None,
                };

                let args = args
                    .clone()
                    .unwrap_or_default()
                    .iter()
                    .map(|expr| {
                        Ok(SqlFunctionArg::Unnamed(FunctionArgExpr::Expr(
                            expr.to_sql()?,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![function_ident]),
                    args,
                    over: None,
                    distinct: false,
                }))
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
            ScalarValue::Time32Second(_) => Err(VegaFusionError::internal(
                "Time32Second cannot be converted to SQL",
            )),
            ScalarValue::Time32Millisecond(_) => Err(VegaFusionError::internal(
                "Time32Millisecond cannot be converted to SQL",
            )),
            ScalarValue::Time64Microsecond(_) => Err(VegaFusionError::internal(
                "Time64Microsecond cannot be converted to SQL",
            )),
            ScalarValue::Time64Nanosecond(_) => Err(VegaFusionError::internal(
                "Time64Nanosecond cannot be converted to SQL",
            )),
        }
    }
}
