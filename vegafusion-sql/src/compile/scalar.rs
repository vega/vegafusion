use crate::compile::data_type::ToSqlDataType;
use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::DFSchema;
use datafusion_expr::{
    expr, lit, ColumnarValue, Expr, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF,
    Signature, Volatility,
};
use sqlparser::ast::{
    Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg, FunctionArgExpr,
    Ident, ObjectName as SqlObjectName, Value as SqlValue,
};
use std::ops::Add;
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

pub trait ToSqlScalar {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlExpr>;
}

impl ToSqlScalar for ScalarValue {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlExpr> {
        match self {
            ScalarValue::Null => Ok(SqlExpr::Value(SqlValue::Null)),
            ScalarValue::Boolean(v) => Ok(SqlExpr::Value(
                v.map(SqlValue::Boolean).unwrap_or(SqlValue::Null),
            )),
            ScalarValue::Float32(v) => v
                .map(|v| {
                    let repr = if !v.is_finite() {
                        // Wrap inf, -inf, and nan in explicit cast
                        return if dialect.supports_non_finite_floats {
                            let cast_dtype = if let Some(dtype) =
                                dialect.cast_datatypes.get(&DataType::Float32)
                            {
                                dtype.clone()
                            } else {
                                return Err(VegaFusionError::sql_not_supported(
                                    "Dialect does not support a Float32 data type",
                                ));
                            };
                            Ok(SqlExpr::Cast {
                                expr: Box::new(SqlExpr::Value(SqlValue::Number(
                                    format!("'{v}'"),
                                    false,
                                ))),
                                data_type: cast_dtype,
                            })
                        } else {
                            Ok(SqlExpr::Value(SqlValue::Null))
                        };
                    } else if v.fract() == 0.0 {
                        format!("{v:.1}")
                    } else {
                        v.to_string()
                    };
                    Ok(SqlExpr::Value(SqlValue::Number(repr, false)))
                })
                .unwrap_or(Ok(SqlExpr::Value(SqlValue::Null))),
            ScalarValue::Float64(v) => v
                .map(|v| {
                    let repr = if !v.is_finite() {
                        return if dialect.supports_non_finite_floats {
                            // Wrap inf, -inf, and nan in explicit cast
                            let cast_dtype = if let Some(dtype) =
                                dialect.cast_datatypes.get(&DataType::Float64)
                            {
                                dtype.clone()
                            } else {
                                return Err(VegaFusionError::sql_not_supported(
                                    "Dialect does not support a Float64 data type",
                                ));
                            };
                            Ok(SqlExpr::Cast {
                                expr: Box::new(SqlExpr::Value(SqlValue::Number(
                                    format!("'{v}'"),
                                    false,
                                ))),
                                data_type: cast_dtype,
                            })
                        } else {
                            Ok(SqlExpr::Value(SqlValue::Null))
                        };
                    } else if v.fract() == 0.0 {
                        format!("{v:.1}")
                    } else {
                        v.to_string()
                    };
                    Ok(SqlExpr::Value(SqlValue::Number(repr, false)))
                })
                .unwrap_or(Ok(SqlExpr::Value(SqlValue::Null))),
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
                            expr.to_sql(dialect)?,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![function_ident]),
                    args,
                    over: None,
                    distinct: false,
                    special: false,
                    order_by: Default::default(),
                }))
            }
            ScalarValue::Date32(v) => date32_to_date(v, dialect),
            ScalarValue::Date64(_) => Err(VegaFusionError::internal(
                "Date64 cannot be converted to SQL",
            )),
            ScalarValue::TimestampSecond(v, _) => {
                if let Some(v) = v {
                    Ok(ms_to_timestamp(v * 1000, dialect)?)
                } else {
                    Ok(SqlExpr::Value(SqlValue::Null))
                }
            }
            ScalarValue::TimestampMillisecond(v, _) => {
                if let Some(v) = v {
                    Ok(ms_to_timestamp(*v, dialect)?)
                } else {
                    Ok(SqlExpr::Value(SqlValue::Null))
                }
            }
            ScalarValue::TimestampMicrosecond(v, _) => {
                if let Some(v) = v {
                    Ok(ms_to_timestamp(v / 1000, dialect)?)
                } else {
                    Ok(SqlExpr::Value(SqlValue::Null))
                }
            }
            ScalarValue::TimestampNanosecond(v, _) => {
                if let Some(v) = v {
                    Ok(ms_to_timestamp(v / 1000000, dialect)?)
                } else {
                    Ok(SqlExpr::Value(SqlValue::Null))
                }
            }
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
            ScalarValue::Decimal256(_, _, _) => Err(VegaFusionError::internal(
                "Decimal256 cannot be converted to SQL",
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
            ScalarValue::Fixedsizelist(_, _, _) => Err(VegaFusionError::internal(
                "Fixedsizelist cannot be converted to SQL",
            )),
            ScalarValue::DurationSecond(_) => Err(VegaFusionError::internal(
                "DurationSecond cannot be converted to SQL",
            )),
            ScalarValue::DurationMillisecond(_) => Err(VegaFusionError::internal(
                "DurationMillisecond cannot be converted to SQL",
            )),
            ScalarValue::DurationMicrosecond(_) => Err(VegaFusionError::internal(
                "DurationMicrosecond cannot be converted to SQL",
            )),
            ScalarValue::DurationNanosecond(_) => Err(VegaFusionError::internal(
                "DurationNanosecond cannot be converted to SQL",
            )),
        }
    }
}

fn ms_to_timestamp(v: i64, dialect: &Dialect) -> Result<SqlExpr> {
    // Hack to recursively transform the epoch_ms_to_utc_timestamp
    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));
    let signature: Signature = Signature::exact(vec![DataType::Int64], Volatility::Immutable);
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        panic!("Placeholder UDF implementation should not be called")
    });

    let udf = ScalarUDF::new(
        "epoch_ms_to_utc_timestamp",
        &signature,
        &return_type,
        &scalar_fn,
    );
    Expr::ScalarUDF(expr::ScalarUDF {
        fun: Arc::new(udf),
        args: vec![lit(v)],
    })
    .to_sql(dialect, &DFSchema::empty())
}

fn date32_to_date(days: &Option<i32>, dialect: &Dialect) -> Result<SqlExpr> {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    match days {
        None => Ok(SqlExpr::Cast {
            expr: Box::new(ScalarValue::Utf8(None).to_sql(dialect)?),
            data_type: DataType::Date32.to_sql(dialect)?,
        }),
        Some(days) => {
            let date = epoch.add(chrono::Duration::days(*days as i64));
            let date_str = date.format("%F").to_string();
            Ok(SqlExpr::Cast {
                expr: Box::new(ScalarValue::from(date_str.as_str()).to_sql(dialect)?),
                data_type: DataType::Date32.to_sql(dialect)?,
            })
        }
    }
}
