use crate::compile::expr::ToSqlExpr;
use crate::dialect::utils::make_utc_expr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    DateTimeField as SqlDateTimeField, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident as SqlIdent, ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_date_part_tz_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(String, SqlExpr, SqlExpr)> {
    if args.len() != 3 {
        return Err(VegaFusionError::sql_not_supported(
            "date_part_tz requires exactly three arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    let sql_arg2 = args[2].to_sql(dialect, schema)?;

    let part = if let SqlExpr::Value(SqlValue::SingleQuotedString(part)) = sql_arg0 {
        part
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "First argument to date_part_tz must be a string literal",
        ));
    };

    Ok((part, sql_arg1, sql_arg2))
}

pub fn at_time_zone_if_not_utc(
    arg: SqlExpr,
    time_zone: SqlExpr,
    naive_timestamps: bool,
) -> SqlExpr {
    let utc = make_utc_expr();
    if time_zone == utc {
        arg
    } else if naive_timestamps {
        SqlExpr::AtTimeZone {
            timestamp: Box::new(SqlExpr::AtTimeZone {
                timestamp: Box::new(arg),
                time_zone: Box::new(utc),
            }),
            time_zone: Box::new(time_zone),
        }
    } else {
        SqlExpr::AtTimeZone {
            timestamp: Box::new(arg),
            time_zone: Box::new(time_zone),
        }
    }
}

pub fn part_to_date_time_field(part: &str) -> Result<SqlDateTimeField> {
    Ok(match part.to_ascii_lowercase().as_str() {
        "year" | "years" => SqlDateTimeField::Year,
        "month" | "months " => SqlDateTimeField::Month,
        "week" | "weeks" => SqlDateTimeField::Week(None),
        "day" | "days" => SqlDateTimeField::Day,
        "date" => SqlDateTimeField::Date,
        "hour" | "hours" => SqlDateTimeField::Hour,
        "minute" | "minutes" => SqlDateTimeField::Minute,
        "second" | "seconds" => SqlDateTimeField::Second,
        "millisecond" | "milliseconds" => SqlDateTimeField::Millisecond,
        _ => {
            return Err(VegaFusionError::sql_not_supported(format!(
                "Unsupported date part to date_part_tz: {part}"
            )))
        }
    })
}

/// Convert date_part_tz(part, ts, tz) ->
///     date_part(part, ts AT TIME ZONE 'UTC' AT TIME ZONE tz)
/// or if tz = 'UTC'
///     date_part(part, ts)
#[derive(Clone, Debug)]
pub struct DatePartTzWithDatePartAndAtTimezoneTransformer {
    naive_timestamps: bool,
}

impl DatePartTzWithDatePartAndAtTimezoneTransformer {
    pub fn new_dyn(naive_timestamps: bool) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self { naive_timestamps })
    }
}

impl FunctionTransformer for DatePartTzWithDatePartAndAtTimezoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let timestamp_in_tz = at_time_zone_if_not_utc(sql_arg1, time_zone, self.naive_timestamps);

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(part),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
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

/// Convert date_part_tz(part, ts, tz) ->
///     extract(part from ts AT TIME ZONE 'UTC' AT TIME ZONE tz)
/// or if tz = 'UTC'
///     extract(part from ts)
#[derive(Clone, Debug)]
pub struct DatePartTzWithExtractAndAtTimezoneTransformer {
    naive_timestamps: bool,
}

impl DatePartTzWithExtractAndAtTimezoneTransformer {
    pub fn new_dyn(naive_timestamps: bool) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self { naive_timestamps })
    }
}

impl FunctionTransformer for DatePartTzWithExtractAndAtTimezoneTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let timestamp_in_tz = at_time_zone_if_not_utc(sql_arg1, time_zone, self.naive_timestamps);

        let field = part_to_date_time_field(&part)?;
        Ok(SqlExpr::Extract {
            field,
            expr: Box::new(timestamp_in_tz),
        })
    }
}

/// Convert date_part_tz(part, ts, tz) ->
///     toHour(toTimeZone(ts, tz))
#[derive(Clone, Debug)]
pub struct DatePartTzClickhouseTransformer;

impl DatePartTzClickhouseTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DatePartTzClickhouseTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let to_timezone_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "toTimeZone".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
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

        let part_function = match part.to_ascii_lowercase().as_str() {
            "year" => "toYear",
            "month" => "toMonth",
            "week" => "toWeek", // TODO: What mode should this be
            "day" => "toDayOfWeek",
            "date" => "toDayOfMonth",
            "hour" => "toHour",
            "minute" => "toMinute",
            "second" => "toSecond",
            _ => {
                return Err(VegaFusionError::sql_not_supported(format!(
                    "Unsupported date part to date_part_tz: {part}"
                )))
            }
        };

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: part_function.to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                    to_timezone_expr,
                ))],
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

/// Convert date_part_tz(part, ts, tz) ->
///     date_part(part, from_utc_timestamp(ts, tz))
/// or if tz = 'UTC'
///     date_part(part, ts)
#[derive(Clone, Debug)]
pub struct DatePartTzWithFromUtcAndDatePartTransformer;

impl DatePartTzWithFromUtcAndDatePartTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DatePartTzWithFromUtcAndDatePartTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        let timestamp_in_tz = if time_zone == utc {
            sql_arg1
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "from_utc_timestamp".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
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

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(part),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
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

#[derive(Clone, Debug)]
pub struct DatePartTzMySqlTransformer;

impl DatePartTzMySqlTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DatePartTzMySqlTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        let timestamp_in_tz = if time_zone == utc {
            sql_arg1
        } else {
            SqlExpr::Function(SqlFunction {
                name: SqlObjectName(vec![SqlIdent {
                    value: "convert_tz".to_string(),
                    quote_style: None,
                }]),
                args: FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(utc)),
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

        let field = part_to_date_time_field(&part)?;
        Ok(SqlExpr::Extract {
            field,
            expr: Box::new(timestamp_in_tz),
        })
    }
}

#[derive(Clone, Debug)]
pub struct DatePartTzSnowflakeTransformer;

impl DatePartTzSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for DatePartTzSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let (part, sql_arg1, time_zone) = process_date_part_tz_args(args, dialect, schema)?;
        let utc = make_utc_expr();
        let timestamp_in_tz = if time_zone == utc {
            sql_arg1
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
                        SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(sql_arg1)),
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

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString(part),
                    ))),
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(timestamp_in_tz)),
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
