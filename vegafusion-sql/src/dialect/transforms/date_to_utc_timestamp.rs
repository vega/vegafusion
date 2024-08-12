use crate::compile::expr::ToSqlExpr;
use crate::dialect::utils::make_utc_expr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    CastKind, DataType as SqlDataType, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident as SqlIdent, ObjectName as SqlObjectName,
    TimezoneInfo as SqlTimezoneInfo, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_date_to_utc_timestamp_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(SqlExpr, SqlExpr)> {
    if args.len() != 2 {
        return Err(VegaFusionError::sql_not_supported(
            "to_utc_timestamp requires exactly two arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    Ok((sql_arg0, sql_arg1))
}

/// Convert date_to_utc_timestamp(d, tz) ->
///     CAST(d as TIMESTAMP) AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     CAST(d as TIMESTAMP)
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampWithCastAndAtTimeZoneTransformer;

impl DateToUtcTimestampWithCastAndAtTimeZoneTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateToUtcTimestampWithCastAndAtTimeZoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        let timestamp_arg = SqlExpr::Cast {
            expr: Box::new(date_arg),
            data_type: SqlDataType::Timestamp(None, SqlTimezoneInfo::None),
            format: None,
            kind: CastKind::Cast,
        };
        let utc = make_utc_expr();
        let utc_timestamp = if time_zone == utc {
            timestamp_arg
        } else {
            let at_tz_expr = SqlExpr::AtTimeZone {
                timestamp: Box::new(timestamp_arg),
                time_zone: Box::new(time_zone),
            };
            SqlExpr::AtTimeZone {
                timestamp: Box::new(at_tz_expr),
                time_zone: Box::new(utc),
            }
        };

        Ok(utc_timestamp)
    }
}

/// Convert date_to_utc_timestamp(d, tz) ->
///     CONVERT_TIMEZONE(tz, 'UTC', CAST(d as TIMESTAMP_NTZ))
/// or if tz = 'UTC'
///     CAST(d as TIMESTAMP_NTZ)
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampSnowflakeTransform;

impl DateToUtcTimestampSnowflakeTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateToUtcTimestampSnowflakeTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        let cast_timestamp_ntz_expr = SqlExpr::Cast {
            expr: Box::new(date_arg),
            data_type: SqlDataType::Custom(
                SqlObjectName(vec![SqlIdent {
                    value: "timestamp_ntz".to_string(),
                    quote_style: None,
                }]),
                Vec::new(),
            ),
            format: None,
            kind: CastKind::Cast,
        };
        let utc = make_utc_expr();
        if time_zone == utc {
            // No conversion needed
            Ok(cast_timestamp_ntz_expr)
        } else {
            let convert_tz_expr = SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_timezone".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                            SqlValue::SingleQuotedString("UTC".to_string()),
                        ))),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(cast_timestamp_ntz_expr)),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: FunctionArguments::None,
            });

            Ok(convert_tz_expr)
        }
    }
}

/// Convert date_to_utc_timestamp(ts, tz) ->
///     function_name(ts, tz)
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampWithFunctionTransformer {
    function_name: String,
}

impl DateToUtcTimestampWithFunctionTransformer {
    pub fn new_dyn(function_name: &str) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self {
            function_name: function_name.to_string(),
        })
    }
}

impl FunctionTransformer for DateToUtcTimestampWithFunctionTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: self.function_name.clone(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_arg)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        }))
    }
}

/// Convert date_to_utc_timestamp(ts, tz) ->
///     function_name(CAST(ts as TIMESTAMP), tz)
/// or
///     function_name(CAST(ts as TIMESTAMP), tz) AT TIME ZONE 'UTC'
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampWithCastFunctionAtTransformer {
    timestamp_type: SqlDataType,
    function_name: String,
    at_timezone_utc: bool,
}

impl DateToUtcTimestampWithCastFunctionAtTransformer {
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

impl FunctionTransformer for DateToUtcTimestampWithCastFunctionAtTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        let cast_expr = SqlExpr::Cast {
            expr: Box::new(date_arg),
            data_type: self.timestamp_type.clone(),
            format: None,
            kind: CastKind::Cast,
        };
        let fn_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: self.function_name.clone(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(cast_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });
        let utc = make_utc_expr();
        if self.at_timezone_utc {
            Ok(SqlExpr::AtTimeZone {
                timestamp: Box::new(fn_expr),
                time_zone: Box::new(utc),
            })
        } else {
            Ok(fn_expr)
        }
    }
}

/// Convert date_to_utc_timestamp(ts, tz) ->
///     toTimeZone(toDateTime(ts, tz), 'UTC')
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampClickhouseTransformer;

impl DateToUtcTimestampClickhouseTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateToUtcTimestampClickhouseTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        let to_date_time_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "toDateTime".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_arg)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });

        let to_time_zone_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "toTimeZone".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(to_date_time_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                ],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });

        Ok(to_time_zone_expr)
    }
}

/// Convert date_to_utc_timestamp(ts, tz) ->
///     convert_timezone(timestamp(ts), tz, 'UTC')
/// or if tz = 'UTC'
///     timestamp(ts)
#[derive(Clone, Debug)]
pub struct DateToUtcTimestampMySqlTransformer;

impl DateToUtcTimestampMySqlTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DateToUtcTimestampMySqlTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (date_arg, time_zone) = process_date_to_utc_timestamp_args(args, dialect, schema)?;

        let timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_arg))],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });
        let utc = make_utc_expr();
        if time_zone == utc {
            // No conversion needed
            Ok(timestamp_expr)
        } else {
            let convert_tz_expr = SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_tz".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_expr)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                            SqlValue::SingleQuotedString("UTC".to_string()),
                        ))),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: FunctionArguments::None,
            });

            Ok(convert_tz_expr)
        }
    }
}
