use crate::compile::expr::ToSqlExpr;
use crate::dialect::utils::make_utc_expr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
    FunctionArgExpr as SqlFunctionArgExpr, FunctionArgumentList, FunctionArguments,
    Ident as SqlIdent, ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_utc_timestamp_to_str_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(SqlExpr, SqlExpr)> {
    if args.len() != 2 {
        return Err(VegaFusionError::sql_not_supported(
            "str_to_utc_timestamp requires exactly two arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    Ok((sql_arg0, sql_arg1))
}

/// Convert utc_timestamp_to_str(ts, tz) ->
///     format_datetime('%Y-%m-%dT%H:%M:%E3S', datetime(ts, tz))
#[derive(Clone, Debug)]
pub struct UtcTimestampToStrBigQueryTransformer;

impl UtcTimestampToStrBigQueryTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToStrBigQueryTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_expr, time_zone) = process_utc_timestamp_to_str_args(args, dialect, schema)?;

        let datetime_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "datetime".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
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

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "format_datetime".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("%Y-%m-%dT%H:%M:%E3S".to_string()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(datetime_expr)),
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

/// Convert utc_timestamp_to_str(ts, tz) ->
///     replace(date_format(from_utc_timestamp(ts, tz), 'y-MM-dd HH:mm:ss.SSS'), ' ', 'T')
/// or if tz == 'UTC'
///     replace(date_format(ts, 'y-MM-dd HH:mm:ss.SSS'), ' ', 'T')
#[derive(Clone, Debug)]
pub struct UtcTimestampToStrDatabricksTransformer;

impl UtcTimestampToStrDatabricksTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToStrDatabricksTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_expr, time_zone) = process_utc_timestamp_to_str_args(args, dialect, schema)?;

        let utc = make_utc_expr();
        let ts_in_tz_expr = if time_zone == utc {
            ts_expr
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "from_utc_timestamp".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
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
            })
        };

        let date_format_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_format".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_in_tz_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("y-MM-dd HH:mm:ss.SSS".to_string()),
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

        // There should be a better way to do this, but including the "T" directly in the format
        // string is an error and I haven't been able to figure out how to escape it.
        let replace_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "replace".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_format_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(" ".to_string()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("T".to_string()),
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

        Ok(replace_expr)
    }
}

/// Convert utc_timestamp_to_str(ts, tz) ->
///     strftime(ts AT TIME ZONE 'UTC' AT TIME ZONE tz,, '%Y-%m-%dT%H:%M:%S.%g'))
/// or if tz == 'UTC'
///     strftime(ts, '%Y-%m-%dT%H:%M:%S.%g')
#[derive(Clone, Debug)]
pub struct UtcTimestampToStrDuckDBTransformer;

impl UtcTimestampToStrDuckDBTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToStrDuckDBTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_expr, time_zone) = process_utc_timestamp_to_str_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        let utc_expr = if time_zone == utc {
            ts_expr
        } else {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(SqlExpr::AtTimeZone {
                    timestamp: Box::new(ts_expr),
                    time_zone: Box::new(utc),
                }),
                time_zone: Box::new(time_zone),
            }
        };

        let strftime_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "strftime".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(utc_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("%Y-%m-%dT%H:%M:%S.%g".to_string()),
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

        Ok(strftime_expr)
    }
}

/// Convert utc_timestamp_to_str(ts, tz) ->
///     to_char(ts AT TIME ZONE 'UTC' AT TIME ZONE tz, 'YYYY-MM-DD"T"HH24:MI:SS.MS'))
/// or if tz == 'UTC'
///     to_char(ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS')
#[derive(Clone, Debug)]
pub struct UtcTimestampToStrPostgresTransformer;

impl UtcTimestampToStrPostgresTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToStrPostgresTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_expr, time_zone) = process_utc_timestamp_to_str_args(args, dialect, schema)?;

        let utc = make_utc_expr();
        let utc_expr = if time_zone == utc {
            ts_expr
        } else {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(SqlExpr::AtTimeZone {
                    timestamp: Box::new(ts_expr),
                    time_zone: Box::new(utc),
                }),
                time_zone: Box::new(time_zone),
            }
        };

        let strftime_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "to_char".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(utc_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("YYYY-MM-DD\"T\"HH24:MI:SS.MS".to_string()),
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

        Ok(strftime_expr)
    }
}

/// Convert utc_timestamp_to_str(ts, tz) ->
///     to_varchar(convert_timezone('UTC', tz, ts), 'y-MM-dd HH:mm:ss.SSS')
/// or if tz == 'UTC'
///     to_varchar(ts, 'y-MM-dd HH:mm:ss.SSS')
#[derive(Clone, Debug)]
pub struct UtcTimestampToStrSnowflakeTransformer;

impl UtcTimestampToStrSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToStrSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_expr, time_zone) = process_utc_timestamp_to_str_args(args, dialect, schema)?;

        let utc = make_utc_expr();
        let ts_in_tz_expr = if time_zone == utc {
            ts_expr
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_timezone".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(utc)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_expr)),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: FunctionArguments::None,
            })
        };

        let date_format_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "to_varchar".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_in_tz_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("YYYY-MM-DD\"T\"HH24:MI:SS.FF3".to_string()),
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

        Ok(date_format_expr)
    }
}
