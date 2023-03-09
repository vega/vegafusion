use crate::compile::expr::ToSqlExpr;
use crate::dialect::{Dialect, FunctionTransformer};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, TimezoneInfo as SqlTimezoneInfo, Value as SqlValue,
};
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

fn process_date_to_utc_timestamp_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<(SqlExpr, String)> {
    if args.len() != 2 {
        return Err(VegaFusionError::sql_not_supported(
            "to_utc_timestamp requires exactly two arguments",
        ));
    }
    let sql_arg0 = args[0].to_sql(dialect, schema)?;
    let sql_arg1 = args[1].to_sql(dialect, schema)?;
    let time_zone = if let SqlExpr::Value(SqlValue::SingleQuotedString(timezone)) = sql_arg1 {
        timezone
    } else {
        return Err(VegaFusionError::sql_not_supported(
            "Second argument to to_utc_timestamp must be a string literal",
        ));
    };
    Ok((sql_arg0, time_zone))
}

/// Convert to_utc_timestamp(d, tz) ->
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
        };

        let utc_timestamp = if time_zone == "UTC" {
            timestamp_arg
        } else {
            let at_tz_expr = SqlExpr::AtTimeZone {
                timestamp: Box::new(timestamp_arg),
                time_zone,
            };
            SqlExpr::AtTimeZone {
                timestamp: Box::new(at_tz_expr),
                time_zone: "UTC".to_string(),
            }
        };

        Ok(utc_timestamp)
    }
}
