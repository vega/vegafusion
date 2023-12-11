use crate::compile::expr::ToSqlExpr;
use crate::dialect::transforms::date_part_tz::{at_time_zone_if_not_utc, part_to_date_time_field};
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr, Ident as SqlIdent,
    ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, ToExternalError, VegaFusionError};

fn process_date_add_tz_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(String, String, SqlExpr, String)> {
    if args.len() != 4 {
        return Err(VegaFusionError::sql_not_supported(
            "date_add_tz requires exactly four arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    let sql_arg2 = args[2].to_sql(dialect, schema)?;
    let sql_arg3 = args[3].to_sql(dialect, schema)?;

    let part = if let SqlExpr::Value(SqlValue::SingleQuotedString(part)) = sql_arg0 {
        part.to_ascii_lowercase()
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "First argument to date_add_tz must be a string literal",
        ));
    };

    let n_str = if let SqlExpr::Value(SqlValue::Number(n, _)) = sql_arg1 {
        n
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Second arg to date_add must be an integer literal",
        ));
    };
    let n_int = n_str
        .parse::<i32>()
        .external("Failed to parse interval step as integer")?;

    let time_zone = if let SqlExpr::Value(SqlValue::SingleQuotedString(timezone)) = sql_arg3 {
        timezone
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Forth argument to date_add_tz must be a string literal",
        ));
    };

    // Handle special cases for intervals
    let (n_int, part) = match part.as_str() {
        "week" => (n_int * 7, "day".to_string()),
        "date" => (n_int, "day".to_string()),
        _ => (n_int, part),
    };

    Ok((part, n_int.to_string(), sql_arg2, time_zone))
}

fn maybe_from_utc(ts_expr: SqlExpr, time_zone: &str) -> SqlExpr {
    if time_zone == "UTC" {
        ts_expr
    } else {
        SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "from_utc_timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone.to_string()),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        })
    }
}

fn maybe_to_utc(ts_expr: SqlExpr, time_zone: &str) -> SqlExpr {
    if time_zone == "UTC" {
        ts_expr
    } else {
        SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "to_utc_timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone.to_string()),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        })
    }
}

/// Convert date_add_tz(part, n, ts, tz) ->
///     to_utc_timestamp(from_utc_timestamp(ts, tz) + Interval '{n} {part}', tz)
/// or if tz = 'UTC'
///     tz + Interval '{n} {part}'
#[derive(Clone, Debug)]
pub struct DateAddTzBigQueryTransformer;

impl DateAddTzBigQueryTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateAddTzBigQueryTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, n_str, ts_expr, time_zone) = process_date_add_tz_args(args, dialect, schema)?;

        let datetime_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "datetime".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone.clone()),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let date_time_field = part_to_date_time_field(&part)?;
        let datetime_add_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "datetime_add".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(datetime_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Interval(
                    sqlparser::ast::Interval {
                        value: Box::new(SqlExpr::Value(SqlValue::Number(n_str, false))),
                        leading_field: Some(date_time_field),
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None,
                    },
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(datetime_add_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(timestamp_expr)
    }
}

/// Convert date_add_tz(part, n, ts, tz) ->
///     to_utc_timestamp(from_utc_timestamp(ts, tz) + Interval '{n} {part}', tz)
/// or if tz = 'UTC'
///     tz + Interval '{n} {part}'
#[derive(Clone, Debug)]
pub struct DateAddTzDatafusionTransformer;

impl DateAddTzDatafusionTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateAddTzDatafusionTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, n_str, ts_expr, time_zone) = process_date_add_tz_args(args, dialect, schema)?;

        let ts_in_tz_expr = maybe_from_utc(ts_expr, &time_zone);

        let date_time_field = part_to_date_time_field(&part)?;
        let interval = SqlExpr::Interval(sqlparser::ast::Interval {
            value: Box::new(SqlExpr::Value(SqlValue::SingleQuotedString(n_str))),
            leading_field: Some(date_time_field),
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });

        let addition_expr = SqlExpr::BinaryOp {
            left: Box::new(ts_in_tz_expr),
            op: SqlBinaryOperator::Plus,
            right: Box::new(interval),
        };

        Ok(maybe_to_utc(addition_expr, &time_zone))
    }
}

/// Convert date_add_tz(part, n, ts, tz) ->
///     to_utc_timestamp(from_utc_timestamp(ts, tz) + Interval '{n} {part}', tz)
/// or if tz = 'UTC'
///     tz + Interval '{n} {part}'
#[derive(Clone, Debug)]
pub struct DateAddTzDatabricksTransformer;

impl DateAddTzDatabricksTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateAddTzDatabricksTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, n_str, ts_expr, time_zone) = process_date_add_tz_args(args, dialect, schema)?;

        let ts_in_tz_expr = maybe_from_utc(ts_expr, &time_zone);

        let shifted_tz_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "dateadd".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(SqlIdent {
                    value: part,
                    quote_style: None,
                }))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::Number(n_str, false),
                ))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_in_tz_expr)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(maybe_to_utc(shifted_tz_expr, &time_zone))
    }
}

/// Convert date_add_tz(part, n, ts, tz) ->
///     ts AT TIME ZONE 'UTC' AT TIME ZONE tz + Interval '{n} {part}' AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     tz + Interval '{n} {part}'
#[derive(Clone, Debug)]
pub struct DateAddTzWithAtTimeZoneIntervalTransformer;

impl DateAddTzWithAtTimeZoneIntervalTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateAddTzWithAtTimeZoneIntervalTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, n_str, ts_expr, time_zone) = process_date_add_tz_args(args, dialect, schema)?;

        let ts_in_tz_expr = at_time_zone_if_not_utc(ts_expr, time_zone.clone(), true);

        let interval_string = format!("{n_str} {part}");
        let interval = SqlExpr::Interval(sqlparser::ast::Interval {
            value: Box::new(SqlExpr::Value(SqlValue::SingleQuotedString(
                interval_string,
            ))),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });

        let addition_expr = SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Nested(Box::new(ts_in_tz_expr))),
            op: SqlBinaryOperator::Plus,
            right: Box::new(interval),
        };

        Ok(if time_zone == "UTC" {
            addition_expr
        } else {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(SqlExpr::AtTimeZone {
                    timestamp: Box::new(SqlExpr::Nested(Box::new(addition_expr))),
                    time_zone,
                }),
                time_zone: "UTC".to_string(),
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct DateAddTzSnowflakeTransformer;

impl DateAddTzSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateAddTzSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, n_str, ts_expr, time_zone) = process_date_add_tz_args(args, dialect, schema)?;

        let timestamp_in_tz = if time_zone == "UTC" {
            ts_expr
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
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        let date_add_in_tz = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestampadd".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(SqlIdent {
                    value: part,
                    quote_style: None,
                }))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::Number(n_str, false),
                ))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let date_add_in_utc = if time_zone == "UTC" {
            date_add_in_tz
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
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_add_in_tz)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(date_add_in_utc)
    }
}
