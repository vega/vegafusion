use crate::compile::expr::ToSqlExpr;
use crate::dialect::utils::make_utc_expr;
use crate::dialect::{Dialect, FunctionTransformer};
use arrow::datatypes::DataType;
use datafusion_common::DFSchema;
use datafusion_expr::{Expr, ExprSchemable};
use sqlparser::ast::{
    Expr as SqlExpr, Function as SqlFunction, FunctionArg as SqlFunctionArg,
    FunctionArgExpr as SqlFunctionArgExpr, FunctionArgumentList, FunctionArguments,
    Ident as SqlIdent, ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_to_utc_timestamp_args(
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

/// Convert to_utc_timestamp(ts, tz) ->
///     ts AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     ts
#[derive(Clone, Debug)]
pub struct ToUtcTimestampWithAtTimeZoneTransformer;

impl ToUtcTimestampWithAtTimeZoneTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for ToUtcTimestampWithAtTimeZoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let dtype = args[0].get_type(schema)?;

        let timestamps_tz = if let DataType::Timestamp(_, tz) = dtype {
            // Explicit time zone provided
            tz
        } else {
            // No explicit time zone provided
            None
        };

        let utc = make_utc_expr();
        let (sql_arg0, time_zone) = process_to_utc_timestamp_args(args, dialect, schema)?;

        let utc_expr = if timestamps_tz.is_some() {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(sql_arg0),
                time_zone: Box::new(utc.clone()),
            }
        } else if time_zone == utc {
            sql_arg0
        } else {
            let at_tz_expr = SqlExpr::AtTimeZone {
                timestamp: Box::new(sql_arg0),
                time_zone: Box::new(time_zone),
            };
            SqlExpr::AtTimeZone {
                timestamp: Box::new(at_tz_expr),
                time_zone: Box::new(utc.clone()),
            }
        };

        Ok(utc_expr)
    }
}

/// Convert to_utc_timestamp(ts, tz) ->
///     CONVERT_TIMEZONE(tz, 'UTC', ts)
/// or if tz = 'UTC'
///     ts
#[derive(Clone, Debug)]
pub struct ToUtcTimestampSnowflakeTransform;

impl ToUtcTimestampSnowflakeTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for ToUtcTimestampSnowflakeTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_arg, time_zone) = process_to_utc_timestamp_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        if time_zone == utc {
            // No conversion needed
            Ok(ts_arg)
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
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_arg)),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });

            Ok(convert_tz_expr)
        }
    }
}

/// Convert to_utc_timestamp(ts, tz) ->
///     timestamp(CAST(ts as DATETIME), tz)
/// or if tz = 'UTC'
///     ts
#[derive(Clone, Debug)]
pub struct ToUtcTimestampBigQueryTransform;

impl ToUtcTimestampBigQueryTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for ToUtcTimestampBigQueryTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (ts_arg, time_zone) = process_to_utc_timestamp_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        if time_zone == utc {
            // No conversion needed
            Ok(ts_arg)
        } else {
            let datetime_expr = SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "datetime".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ts_arg))],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            });

            let convert_tz_expr = SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "timestamp".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(datetime_expr)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_zone)),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                within_group: vec![],
                filter: None,
                null_treatment: None,
                over: None,
            });

            Ok(convert_tz_expr)
        }
    }
}
