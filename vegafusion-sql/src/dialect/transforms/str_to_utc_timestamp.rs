use crate::compile::expr::ToSqlExpr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr, Ident as SqlIdent,
    ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_str_to_utc_timestamp_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(SqlExpr, String)> {
    if args.len() != 2 {
        return Err(VegaFusionError::sql_not_supported(
            "str_to_utc_timestamp requires exactly two arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    let time_zone = if let SqlExpr::Value(SqlValue::SingleQuotedString(timezone)) = sql_arg1 {
        timezone
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Second argument to str_to_utc_timestamp must be a string literal",
        ));
    };
    Ok((sql_arg0, time_zone))
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     CAST(ts as TIMESTAMP) AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     CAST(ts as TIMESTAMP)
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampWithCastAndAtTimeZoneTransformer {
    timestamp_type: SqlDataType,
}

impl StrToUtcTimestampWithCastAndAtTimeZoneTransformer {
    pub fn new_dyn(timestamp_type: SqlDataType) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self { timestamp_type })
    }
}

impl FunctionTransformer for StrToUtcTimestampWithCastAndAtTimeZoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        let cast_expr = SqlExpr::Cast {
            expr: Box::new(sql_arg0),
            data_type: self.timestamp_type.clone(),
        };
        let utc_expr = if time_zone == "UTC" {
            cast_expr
        } else {
            let at_tz_expr = SqlExpr::AtTimeZone {
                timestamp: Box::new(cast_expr),
                time_zone,
            };
            SqlExpr::AtTimeZone {
                timestamp: Box::new(at_tz_expr),
                time_zone: "UTC".to_string(),
            }
        };
        Ok(utc_expr)
    }
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     function_name(CAST(ts as TIMESTAMP), tz)
/// or
///     function_name(CAST(ts as TIMESTAMP), tz) AT TIME ZONE 'UTC'
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampWithCastFunctionAtTransformer {
    timestamp_type: SqlDataType,
    function_name: String,
    at_timezone_utc: bool,
}

impl StrToUtcTimestampWithCastFunctionAtTransformer {
    pub fn new_dyn(
        timestamp_type: SqlDataType,
        function_name: &str,
        at_timezone_utc: bool,
    ) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self {
            timestamp_type,
            function_name: function_name.to_string(),
            at_timezone_utc,
        })
    }
}

impl FunctionTransformer for StrToUtcTimestampWithCastFunctionAtTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        let cast_expr = SqlExpr::Cast {
            expr: Box::new(sql_arg0),
            data_type: self.timestamp_type.clone(),
        };
        let fn_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: self.function_name.clone(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(cast_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });
        if self.at_timezone_utc {
            Ok(SqlExpr::AtTimeZone {
                timestamp: Box::new(fn_expr),
                time_zone: "UTC".to_string(),
            })
        } else {
            Ok(fn_expr)
        }
    }
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     function_name(ts, tz)
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampWithFunctionTransformer {
    function_name: String,
}

impl StrToUtcTimestampWithFunctionTransformer {
    pub fn new_dyn(function_name: &str) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self {
            function_name: function_name.to_string(),
        })
    }
}

impl FunctionTransformer for StrToUtcTimestampWithFunctionTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: self.function_name.clone(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg0)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        }))
    }
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     toTimeZone(toDateTime(ts, tz), 'UTC')
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampClickhouseTransformer;

impl StrToUtcTimestampClickhouseTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for StrToUtcTimestampClickhouseTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        let to_date_time_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "toDateTime".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg0)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(time_zone),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let to_time_zone_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "toTimeZone".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(to_date_time_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString("UTC".to_string()),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(to_time_zone_expr)
    }
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     convert_timezone(timestamp(ts), tz, 'UTC')
/// or if tz = 'UTC'
///     timestamp(ts)
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampMySqlTransformer;

impl StrToUtcTimestampMySqlTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for StrToUtcTimestampMySqlTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        let timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg0))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        if time_zone == "UTC" {
            // No conversion needed
            Ok(timestamp_expr)
        } else {
            let convert_tz_expr = SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_tz".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(time_zone),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            });

            Ok(convert_tz_expr)
        }
    }
}

/// Convert str_to_utc_timestamp(ts, tz) ->
///     convert_timezone(tz, 'UTC', CAST(ts as timestamp_ntz))
/// or if tz = 'UTC'
///     CAST(ts as timestamp_ntz)
#[derive(Clone, Debug)]
pub struct StrToUtcTimestampSnowflakeTransformer;

impl StrToUtcTimestampSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for StrToUtcTimestampSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (sql_arg0, time_zone) = process_str_to_utc_timestamp_args(args, dialect, schema)?;

        let cast_timestamp_ntz_expr = SqlExpr::Cast {
            expr: Box::new(sql_arg0),
            data_type: SqlDataType::Custom(
                SqlObjectName(vec![SqlIdent {
                    value: "timestamp_ntz".to_string(),
                    quote_style: None,
                }]),
                Vec::new(),
            ),
        };

        if time_zone == "UTC" {
            // No conversion needed
            Ok(cast_timestamp_ntz_expr)
        } else {
            let convert_tz_expr = SqlExpr::Function(SqlFunction {
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
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(cast_timestamp_ntz_expr)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            });

            Ok(convert_tz_expr)
        }
    }
}
