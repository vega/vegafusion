use crate::compile::expr::ToSqlExpr;
use crate::dialect::transforms::date_part_tz::at_time_zone_if_not_utc;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
    FunctionArgExpr as SqlFunctionArgExpr, Ident as SqlIdent, ObjectName as SqlObjectName,
    Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_date_trunc_tz_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(String, SqlExpr, String)> {
    if args.len() != 3 {
        return Err(VegaFusionError::sql_not_supported(
            "date_trunc_tz requires exactly three arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    let sql_arg2 = args[2].to_sql(dialect, schema)?;

    let part = if let SqlExpr::Value(SqlValue::SingleQuotedString(part)) = sql_arg0 {
        part
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "First argument to date_trunc_tz must be a string literal",
        ));
    };

    let time_zone = if let SqlExpr::Value(SqlValue::SingleQuotedString(timezone)) = sql_arg2 {
        timezone
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Third argument to date_trunc_tz must be a string literal",
        ));
    };
    Ok((part, sql_arg1, time_zone))
}

/// Convert date_trunc_tz(part, ts, tz) ->
///     date_trunc(part, ts AT TIME ZONE 'UTC' AT TIME ZONE tz) AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     date_part(part, ts)
#[derive(Clone, Debug)]
pub struct DateTruncTzWithDateTruncAndAtTimezoneTransformer {
    naive_timestamps: bool,
}

impl DateTruncTzWithDateTruncAndAtTimezoneTransformer {
    pub fn new_dyn(naive_timestamps: bool) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self { naive_timestamps })
    }
}

impl FunctionTransformer for DateTruncTzWithDateTruncAndAtTimezoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_trunc_tz_args(args, dialect, schema)?;
        let timestamp_in_tz =
            at_time_zone_if_not_utc(sql_arg1, time_zone.clone(), self.naive_timestamps);

        let part_func_arg = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
            SqlValue::SingleQuotedString(part),
        )));
        let ts_func_arg = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz));
        let truncated_in_tz = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_trunc".to_string(),
                quote_style: None,
            }]),
            args: vec![part_func_arg, ts_func_arg],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let truncated_in_utc = if time_zone == "UTC" {
            truncated_in_tz
        } else if self.naive_timestamps {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(SqlExpr::AtTimeZone {
                    timestamp: Box::new(truncated_in_tz),
                    time_zone,
                }),
                time_zone: "UTC".to_string(),
            }
        } else {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(truncated_in_tz),
                time_zone: "UTC".to_string(),
            }
        };
        Ok(truncated_in_utc)
    }
}

/// Convert date_trunc_tz(part, ts, tz) ->
///     timestamp_trunc(ts, part, tz)
#[derive(Clone, Debug)]
pub struct DateTruncTzWithTimestampTruncTransformer;

impl DateTruncTzWithTimestampTruncTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateTruncTzWithTimestampTruncTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_trunc_tz_args(args, dialect, schema)?;

        let ts_func_arg = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1));
        let part_func_arg =
            SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(SqlIdent {
                value: part,
                quote_style: None,
            })));
        let tz_func_arg = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
            SqlValue::SingleQuotedString(time_zone),
        )));
        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp_trunc".to_string(),
                quote_style: None,
            }]),
            args: vec![ts_func_arg, part_func_arg, tz_func_arg],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        }))
    }
}

/// Convert date_part_tz(part, ts, tz) ->
///     toStartOfHour(ts, tz)
#[derive(Clone, Debug)]
pub struct DateTruncTzClickhouseTransformer;

impl DateTruncTzClickhouseTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateTruncTzClickhouseTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_trunc_tz_args(args, dialect, schema)?;

        let trunc_function = match part.to_ascii_lowercase().as_str() {
            "year" => "toStartOfYear",
            "month" => "toStartOfMonth",
            "week" => "toStartOfWeek", // TODO: What mode should this be
            "day" => "toStartOfDay",
            "hour" => "toStartOfHour",
            "minute" => "toStartOfMinute",
            "second" => "toStartOfSecond",
            _ => {
                return Err(VegaFusionError::sql_not_supported(format!(
                    "Unsupported date part to date_trunc_tz: {part}"
                )))
            }
        };

        let trunc_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: trunc_function.to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone.clone()),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let in_timezone_expr = if time_zone == "UTC" {
            trunc_expr
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "toTimeZone".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(trunc_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(in_timezone_expr)
    }
}

/// Convert date_trunc_tz(part, ts, tz) ->
///     to_utc_timestamp(date_trunc(part, from_utc_timestamp(ts, tz)), tz)
/// or if tz = 'UTC'
///     date_trunc(part, ts)
#[derive(Clone, Debug)]
pub struct DateTruncTzWithFromUtcAndDateTruncTransformer;

impl DateTruncTzWithFromUtcAndDateTruncTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateTruncTzWithFromUtcAndDateTruncTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_trunc_tz_args(args, dialect, schema)?;

        let timestamp_in_tz = if time_zone == "UTC" {
            sql_arg1
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "from_utc_timestamp".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(time_zone.clone()),
                    ))),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        let date_trunc_in_tz = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_trunc".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(part),
                ))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let date_trunc_in_utc = if time_zone == "UTC" {
            date_trunc_in_tz
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "to_utc_timestamp".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_trunc_in_tz)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(time_zone),
                    ))),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(date_trunc_in_utc)
    }
}

#[derive(Clone, Debug)]
pub struct DateTruncTzSnowflakeTransformer;

impl DateTruncTzSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateTruncTzSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_trunc_tz_args(args, dialect, schema)?;

        let timestamp_in_tz = if time_zone == "UTC" {
            sql_arg1
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_timezone".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(time_zone.clone()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        let date_trunc_in_tz = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_trunc".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(part),
                ))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let date_trunc_in_utc = if time_zone == "UTC" {
            date_trunc_in_tz
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_timezone".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(time_zone),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_trunc_in_tz)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(date_trunc_in_utc)
    }
}
