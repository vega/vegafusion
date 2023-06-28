use crate::compile::expr::ToSqlExpr;
use crate::dialect::{Dialect, FunctionTransformer};
use arrow::datatypes::DataType;
use datafusion_common::DFSchema;
use datafusion_expr::{cast, lit, Expr};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DateTimeField as SqlDateTimeField, Expr as SqlExpr,
    Function as SqlFunction, FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr,
    Ident as SqlIdent, ObjectName as SqlObjectName, Value as SqlValue,
};
use std::ops::{Add, Div};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

struct MakeUtcTimestampSqlArgs {
    pub year: SqlExpr,
    pub month: SqlExpr,
    pub day: SqlExpr,
    pub hour: SqlExpr,
    pub minute: SqlExpr,
    pub second: SqlExpr,
    pub millisecond: SqlExpr,
    pub time_zone: String,
}

fn process_make_utc_timestamp_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
    add_one_to_month: bool,
    fractional_s: bool,
) -> Result<MakeUtcTimestampSqlArgs> {
    if args.len() != 8 {
        return Err(VegaFusionError::sql_not_supported(
            "make_utc_timestamp requires exactly eight arguments",
        ));
    }

    let sql_tz_arg = args[7].to_sql(dialect, schema)?;
    let time_zone = if let SqlExpr::Value(SqlValue::SingleQuotedString(time_zone)) = sql_tz_arg {
        time_zone
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Third argument to make_utc_timestamp must be a string literal",
        ));
    };

    let month = if add_one_to_month {
        // Add one so that month ranges from 1 to 12 instead of 0 to 11
        cast(args[1].clone().add(lit(1)), DataType::Int32).to_sql(dialect, schema)?
    } else {
        args[1].to_sql(dialect, schema)?
    };

    let (second, millisecond) = if fractional_s {
        (
            args[5]
                .clone()
                .add(args[6].clone().div(lit(1000.0)))
                .to_sql(dialect, schema)?,
            lit(0).to_sql(dialect, schema)?,
        )
    } else {
        (
            args[5].to_sql(dialect, schema)?,
            args[6].to_sql(dialect, schema)?,
        )
    };

    Ok(MakeUtcTimestampSqlArgs {
        year: args[0].to_sql(dialect, schema)?,
        month,
        day: args[2].to_sql(dialect, schema)?,
        hour: args[3].to_sql(dialect, schema)?,
        minute: args[4].to_sql(dialect, schema)?,
        second,
        millisecond,
        time_zone,
    })
}

/// Convert make_utc_timestamp(Y, M, d, h, m, s, ms, tz) ->
///     TIMESTAMP(DATETIME(DATE(Y, M + 1, d), TIME_ADD(TIME(h, m, s), INTERVAL ms MILLISECOND)), tz)
#[derive(Clone, Debug)]
pub struct MakeUtcTimestampBigQueryTransformer;

impl MakeUtcTimestampBigQueryTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for MakeUtcTimestampBigQueryTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let sql_args = process_make_utc_timestamp_args(args, dialect, schema, true, false)?;

        let date_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.year)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.month)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.day)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let time_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "time".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.hour)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.minute)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.second)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let time_with_ms_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "time_add".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Interval(
                    sqlparser::ast::Interval {
                        value: Box::new(sql_args.millisecond),
                        leading_field: Some(SqlDateTimeField::Millisecond),
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

        let datetime_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "datetime".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(time_with_ms_expr)),
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
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(datetime_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(sql_args.time_zone),
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

/// Convert make_utc_timestamp(Y, M, d, h, m, s, ms, tz) ->
///     to_utc_timestamp(dateadd(millisecond, ms, make_timestamp(Y, M, d, h, m, s)), tz)
/// or if tz = 'UTC'
///     dateadd(millisecond, ms, make_timestamp(Y, M, d, h, m, s))
#[derive(Clone, Debug)]
pub struct MakeUtcTimestampDatabricksTransformer;

impl MakeUtcTimestampDatabricksTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for MakeUtcTimestampDatabricksTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let sql_args = process_make_utc_timestamp_args(args, dialect, schema, true, false)?;

        let make_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "make_timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.year)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.month)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.day)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.hour)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.minute)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.second)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let date_add_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "dateadd".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(SqlIdent {
                    value: "millisecond".to_string(),
                    quote_style: None,
                }))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.millisecond)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(make_timestamp_expr)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let ts_in_utc_expr = if sql_args.time_zone == "UTC" {
            date_add_expr
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "to_utc_timestamp".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(date_add_expr)),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(sql_args.time_zone),
                    ))),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(ts_in_utc_expr)
    }
}

/// Convert make_utc_timestamp(Y, M, d, h, m, s, ms, tz) ->
///     make_timestamp(Y, M, d, h, m, s + ms / 1000) AT TIME ZONE tz AT TIME ZONE 'UTC'
/// or if tz = 'UTC'
///     make_timestamp(Y, M, d, h, m, s + ms / 1000)
#[derive(Clone, Debug)]
pub struct MakeUtcTimestampDuckDbTransformer;

impl MakeUtcTimestampDuckDbTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for MakeUtcTimestampDuckDbTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let sql_args = process_make_utc_timestamp_args(args, dialect, schema, true, true)?;

        let make_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "make_timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.year)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.month)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.day)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.hour)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.minute)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.second)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let timestamp_in_utc_expr = if sql_args.time_zone == "UTC" {
            make_timestamp_expr
        } else {
            SqlExpr::AtTimeZone {
                timestamp: Box::new(SqlExpr::AtTimeZone {
                    timestamp: Box::new(make_timestamp_expr),
                    time_zone: sql_args.time_zone,
                }),
                time_zone: "UTC".to_string(),
            }
        };

        Ok(timestamp_in_utc_expr)
    }
}

/// Convert make_utc_timestamp(Y, M, d, h, m, s, ms, tz) ->
///     make_timestamptz(Y, M, d, h, m, s + ms / 1000, tz) AT TIME ZONE 'UTC'
#[derive(Clone, Debug)]
pub struct MakeUtcTimestampPostgresTransformer;

impl MakeUtcTimestampPostgresTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for MakeUtcTimestampPostgresTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let sql_args = process_make_utc_timestamp_args(args, dialect, schema, true, true)?;

        let make_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "make_timestamptz".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.year)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.month)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.day)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.hour)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.minute)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.second)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::SingleQuotedString(sql_args.time_zone),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let timestamp_in_utc_expr = SqlExpr::AtTimeZone {
            timestamp: Box::new(make_timestamp_expr),
            time_zone: "UTC".to_string(),
        };

        Ok(timestamp_in_utc_expr)
    }
}

/// Convert make_utc_timestamp(Y, M, d, h, m, s, ms, tz) ->
///     convert_timezone(tz, 'UTC', timestamp_ntz_from_parts(Y, M, d, h, m, s, ms * 1000000))
/// or if tz = 'UTC'
///     timestamp_ntz_from_parts(Y, M, d, h, m, s, ms * 1000000)
#[derive(Clone, Debug)]
pub struct MakeUtcTimestampSnowflakeTransformer;

impl MakeUtcTimestampSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for MakeUtcTimestampSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let sql_args = process_make_utc_timestamp_args(args, dialect, schema, true, false)?;

        let make_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp_ntz_from_parts".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.year)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.month)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.day)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.hour)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.minute)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_args.second)),
                // Convert ms to ns
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::BinaryOp {
                    left: Box::new(sql_args.millisecond),
                    op: SqlBinaryOperator::Multiply,
                    right: Box::new(SqlExpr::Value(SqlValue::Number(
                        "1000000".to_string(),
                        false,
                    ))),
                })),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let timestamp_in_utc_expr = if sql_args.time_zone == "UTC" {
            make_timestamp_expr
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_timezone".to_string(),
                    quote_style: None,
                }]),
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(sql_args.time_zone),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("UTC".to_string()),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(make_timestamp_expr)),
                ],
                over: None,
                distinct: false,
                special: false,
                order_by: Default::default(),
            })
        };

        Ok(timestamp_in_utc_expr)
    }
}
