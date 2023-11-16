use crate::compile::expr::ToSqlExpr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArgExpr as SqlFunctionArgExpr, Ident as SqlIdent,
    ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_epoch_ms_to_utc_timestamp_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<SqlExpr> {
    if args.len() != 1 {
        return Err(VegaFusionError::sql_not_supported(
            "epoch_utc_timestamp requires exactly one argument",
        ));
    }
    args[0].to_sql(dialect, schema)
}

/// Convert epoch_ms_to_utc_timestamp(ms) ->
///     TIMESTAMP_MILLIS(ms)
#[derive(Clone, Debug)]
pub struct EpochMsToUtcTimestampBigQueryTransformer;

impl EpochMsToUtcTimestampBigQueryTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for EpochMsToUtcTimestampBigQueryTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ms_expr = process_epoch_ms_to_utc_timestamp_args(args, dialect, schema)?;

        let ts_millis_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "timestamp_millis".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ms_expr))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(ts_millis_expr)
    }
}

/// Convert epoch_ms_to_utc_timestamp(ms) ->
///     dateadd(millisecond, ms % 1000, from_unixtime(floor(ms / 1000)))
#[derive(Clone, Debug)]
pub struct EpochMsToUtcTimestampDatabricksTransformer;

impl EpochMsToUtcTimestampDatabricksTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for EpochMsToUtcTimestampDatabricksTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ms_expr = process_epoch_ms_to_utc_timestamp_args(args, dialect, schema)?;

        let mod_1000_expr = SqlExpr::BinaryOp {
            left: Box::new(ms_expr.clone()),
            op: SqlBinaryOperator::Modulo,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        let div_1000_expr = SqlExpr::BinaryOp {
            left: Box::new(ms_expr),
            op: SqlBinaryOperator::Divide,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };
        let floor_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "floor".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                div_1000_expr,
            ))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });
        let from_unix_time_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "from_unixtime".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                floor_expr,
            ))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });
        let dateadd_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "dateadd".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(SqlIdent {
                    value: "millisecond".to_string(),
                    quote_style: None,
                }))),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(mod_1000_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(from_unix_time_expr)),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(dateadd_expr)
    }
}

/// Convert epoch_ms_to_utc_timestamp(ms) ->
///     epoch_ms(ms)
#[derive(Clone, Debug)]
pub struct EpochMsToUtcTimestampDuckDbTransformer;

impl EpochMsToUtcTimestampDuckDbTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for EpochMsToUtcTimestampDuckDbTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ms_expr = process_epoch_ms_to_utc_timestamp_args(args, dialect, schema)?;

        let epoch_ms_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "epoch_ms".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ms_expr))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(epoch_ms_expr)
    }
}

/// Convert epoch_ms_to_utc_timestamp(ms) ->
///     dateadd(millisecond, ms % 1000, to_timestamp(floor(ms / 1000)))
#[derive(Clone, Debug)]
pub struct EpochMsToUtcTimestampPostgresTransformer;

impl EpochMsToUtcTimestampPostgresTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for EpochMsToUtcTimestampPostgresTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ms_expr = process_epoch_ms_to_utc_timestamp_args(args, dialect, schema)?;

        let mod_1000_expr = SqlExpr::BinaryOp {
            left: Box::new(ms_expr.clone()),
            op: SqlBinaryOperator::Modulo,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        let div_1000_expr = SqlExpr::BinaryOp {
            left: Box::new(ms_expr),
            op: SqlBinaryOperator::Divide,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };
        let floor_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "floor".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                div_1000_expr,
            ))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });
        let to_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "to_timestamp".to_string(),
                quote_style: None,
            }]),
            args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                floor_expr,
            ))],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        let to_timestamp_at_utc_expr = SqlExpr::AtTimeZone {
            timestamp: Box::new(to_timestamp_expr),
            time_zone: "UTC".to_string(),
        };

        let interval_expr = SqlExpr::Interval(sqlparser::ast::Interval {
            value: Box::new(SqlExpr::Value(SqlValue::SingleQuotedString(
                "1 millisecond".to_string(),
            ))),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        });

        let interval_mult_expr = SqlExpr::BinaryOp {
            left: Box::new(interval_expr),
            op: SqlBinaryOperator::Multiply,
            right: Box::new(SqlExpr::Nested(Box::new(mod_1000_expr))),
        };

        let interval_addition_expr = SqlExpr::BinaryOp {
            left: Box::new(to_timestamp_at_utc_expr),
            op: SqlBinaryOperator::Plus,
            right: Box::new(interval_mult_expr),
        };

        Ok(interval_addition_expr)
    }
}

/// Convert epoch_ms_to_utc_timestamp(ms) ->
///     to_timestamp_ntz(ms)
#[derive(Clone, Debug)]
pub struct EpochMsToUtcTimestampSnowflakeTransformer;

impl EpochMsToUtcTimestampSnowflakeTransformer {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for EpochMsToUtcTimestampSnowflakeTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ms_expr = process_epoch_ms_to_utc_timestamp_args(args, dialect, schema)?;

        let to_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "to_timestamp_ntz".to_string(),
                quote_style: None,
            }]),
            args: vec![
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(ms_expr)),
                SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                    SqlValue::Number("3".to_string(), false),
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: Default::default(),
        });

        Ok(to_timestamp_expr)
    }
}
