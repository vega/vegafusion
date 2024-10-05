use crate::compile::expr::ToSqlExpr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DateTimeField as SqlDateTimeField, Expr as SqlExpr,
    ExtractSyntax, Function as SqlFunction, FunctionArg as SqlFunctionArg,
    FunctionArgExpr as SqlFunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName as SqlObjectName, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_utc_timestamp_to_epoch_ms_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<SqlExpr> {
    if args.len() != 1 {
        return Err(VegaFusionError::sql_not_supported(
            "utc_timestamp_to_epoch_ms requires exactly one argument",
        ));
    }
    args[0].to_sql(dialect, schema)
}

/// Convert utc_timestamp_to_epoch_ms(ts) ->
///     unix_timestamp(ts) * 1000 + (date_part('second', ts) % 1) * 1000
#[derive(Clone, Debug)]
pub struct UtcTimestampToEpochMsDatabricksTransform;

impl UtcTimestampToEpochMsDatabricksTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToEpochMsDatabricksTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ts_expr = process_utc_timestamp_to_epoch_ms_args(args, dialect, schema)?;

        let unix_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "unix_timestamp".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                    ts_expr.clone(),
                ))],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });
        let lhs = SqlExpr::BinaryOp {
            left: Box::new(unix_timestamp_expr),
            op: SqlBinaryOperator::Multiply,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        let date_part_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("second".to_string()),
                    ))),
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
        });
        let mod_1_expr = SqlExpr::Nested(Box::new(SqlExpr::BinaryOp {
            left: Box::new(date_part_expr),
            op: SqlBinaryOperator::Modulo,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1".to_string(), false))),
        }));

        let rhs = SqlExpr::BinaryOp {
            left: Box::new(mod_1_expr),
            op: SqlBinaryOperator::Multiply,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        Ok(SqlExpr::BinaryOp {
            left: Box::new(lhs),
            op: SqlBinaryOperator::Plus,
            right: Box::new(rhs),
        })
    }
}

/// Convert utc_timestamp_to_epoch_ms(ts) ->
///     epoch(ts) * 1000 + date_part('millisecond', ts) % 1000
#[derive(Clone, Debug)]
pub struct UtcTimestampToEpochMsDuckdbTransform;

impl UtcTimestampToEpochMsDuckdbTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToEpochMsDuckdbTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ts_expr = process_utc_timestamp_to_epoch_ms_args(args, dialect, schema)?;

        let unix_timestamp_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "epoch".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                    ts_expr.clone(),
                ))],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });
        let lhs = SqlExpr::BinaryOp {
            left: Box::new(unix_timestamp_expr),
            op: SqlBinaryOperator::Multiply,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        let date_part_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Value(
                        SqlValue::SingleQuotedString("millisecond".to_string()),
                    ))),
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
        });
        let rhs = SqlExpr::BinaryOp {
            left: Box::new(date_part_expr),
            op: SqlBinaryOperator::Modulo,
            right: Box::new(SqlExpr::Value(SqlValue::Number("1000".to_string(), false))),
        };

        Ok(SqlExpr::BinaryOp {
            left: Box::new(lhs),
            op: SqlBinaryOperator::Plus,
            right: Box::new(rhs),
        })
    }
}

/// Convert utc_timestamp_to_epoch_ms(ts) ->
///     floor(extract(epoch from ts) * 1000)
#[derive(Clone, Debug)]
pub struct UtcTimestampToEpochMsPostgresTransform;

impl UtcTimestampToEpochMsPostgresTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToEpochMsPostgresTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ts_expr = process_utc_timestamp_to_epoch_ms_args(args, dialect, schema)?;

        let extract_expr = SqlExpr::Extract {
            field: SqlDateTimeField::Epoch,
            syntax: ExtractSyntax::From,
            expr: Box::new(ts_expr),
        };

        let epoch_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "floor".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                    SqlExpr::BinaryOp {
                        left: Box::new(extract_expr),
                        op: SqlBinaryOperator::Multiply,
                        right: Box::new(SqlExpr::Value(SqlValue::Number(
                            "1000".to_string(),
                            false,
                        ))),
                    },
                ))],
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });

        Ok(epoch_expr)
    }
}

/// Convert utc_timestamp_to_epoch_ms(ts) ->
///     extract(EPOCH_MILLISECOND from ts)
#[derive(Clone, Debug)]
pub struct UtcTimestampToEpochMsSnowflakeTransform;

impl UtcTimestampToEpochMsSnowflakeTransform {
    pub fn new_dyn() -> Arc<dyn FunctionTransformer> {
        Arc::new(Self)
    }
}

impl FunctionTransformer for UtcTimestampToEpochMsSnowflakeTransform {
    fn transform(&self, args: &[Expr], dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        let ts_expr = process_utc_timestamp_to_epoch_ms_args(args, dialect, schema)?;

        let date_part_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![Ident {
                value: "date_part".to_string(),
                quote_style: None,
            }]),
            args: FunctionArguments::List(FunctionArgumentList {
                args: vec![
                    SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(SqlExpr::Identifier(Ident {
                        value: "epoch_millisecond".to_string(),
                        quote_style: None,
                    }))),
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
        });

        Ok(date_part_expr)
    }
}
