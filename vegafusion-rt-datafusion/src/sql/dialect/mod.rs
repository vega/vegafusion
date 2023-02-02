use crate::sql::compile::expr::ToSqlExpr;
use datafusion_expr::Expr;
use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Value as SqlValue};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::error::{Result, VegaFusionError};

#[derive(Clone, Debug)]
pub struct Dialect {
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: char,

    /// Names of supported scalar functions that match the semantics of the DataFusion implementation
    pub scalar_functions: HashSet<String>,

    /// Names of supported aggregate functions that match the semantics of the DataFusion implementation
    pub aggregate_functions: HashSet<String>,

    /// Names of supported window functions that match the semantics of the DataFusion implementation
    pub window_functions: HashSet<String>,

    /// Scalar function transformations
    pub scalar_transformers: HashMap<String, Arc<dyn FunctionTransformer>>,

    /// Aggregate function transformations
    pub aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>>,
}

impl Default for Dialect {
    fn default() -> Self {
        Self {
            quote_style: '"',
            scalar_functions: Default::default(),
            aggregate_functions: Default::default(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers: Default::default(),
        }
    }
}

impl Dialect {
    pub fn datafusion() -> Self {
        let mut scalar_transforms: HashMap<String, Arc<dyn FunctionTransformer>> = HashMap::new();
        scalar_transforms.insert("date_add".to_string(), Arc::new(DateAddToIntervalAddition));

        Self {
            quote_style: '"',
            scalar_functions: vec![
                "abs",
                "acos",
                "asin",
                "atan",
                "atan2",
                "ceil",
                "coalesce",
                "cos",
                "digest",
                "exp",
                "floor",
                "ln",
                "log",
                "log10",
                "log2",
                "pow",
                "round",
                "signum",
                "sin",
                "sqrt",
                "tan",
                "trunc",
                "make_array",
                "ascii",
                "bit_length",
                "btrim",
                "length",
                "chr",
                "concat",
                "concat_ws",
                "date_part",
                "date_trunc",
                "date_bin",
                "initcap",
                "left",
                "lpad",
                "lower",
                "ltrim",
                "md5",
                "nullif",
                "octet_length",
                "random",
                "regexp_replace",
                "repeat",
                "replace",
                "reverse",
                "right",
                "rpad",
                "rtrim",
                "sha224",
                "sha256",
                "sha384",
                "sha512",
                "split_part",
                "starts_with",
                "strpos",
                "substr",
                "to_hex",
                "to_timestamp",
                "to_timestamp_millis",
                "to_timestamp_micros",
                "to_timestamp_seconds",
                "from_unixtime",
                "now",
                "translate",
                "trim",
                "upper",
                "regexp_match",
                "struct",
                "arrow_typeof",
                "current_date",
                "current_time",
                "uuid",
                // UDFs
                "isnan",
                "isfinite",
                "pow",
                "timestamp_to_timestamptz",
                "timestamptz_to_timestamp",
                "date_to_timestamptz",
                "epoch_ms_to_timestamptz",
                "str_to_timestamptz",
                "make_timestamptz",
                "timestamptz_to_epoch_ms",
                "vega_timeunit",
                "format_timestamp",
                "make_list",
                "len",
                "indexof",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            aggregate_functions: vec![
                "min",
                "max",
                "count",
                "avg",
                "sum",
                "median",
                "approx_distinct",
                "array_agg",
                "var",
                "var_pop",
                "stddev",
                "stddev_pop",
                "covar",
                "covar_pop",
                "corr",
                "approx_percentile_cont",
                "approx_percentile_cont_with_weight",
                "approx_median",
                "grouping",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),

            window_functions: vec![
                "row_number",
                "rank",
                "dense_rank",
                "percent_rank",
                "cume_dist",
                "ntile",
                "lag",
                "lead",
                "first_value",
                "last_value",
                "nth_value",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            scalar_transformers: scalar_transforms,
            aggregate_transformers: Default::default(),
        }
    }
}

pub trait FunctionTransformer: Debug + Send + Sync {
    fn transform(&self, args: &[Expr], dialect: &Dialect) -> Result<SqlExpr>;
}

#[derive(Clone, Debug)]
struct DateAddToIntervalAddition;
impl FunctionTransformer for DateAddToIntervalAddition {
    fn transform(&self, args: &[Expr], dialect: &Dialect) -> Result<SqlExpr> {
        // Convert date_add function to interval arithmetic
        if args.len() != 3 {
            return Err(VegaFusionError::sql_not_supported(
                "date_add requires exactly 3 arguments",
            ));
        }

        let date_part = if let Expr::Literal(ScalarValue::Utf8(Some(part))) = &args[0] {
            part.clone()
        } else {
            return Err(VegaFusionError::sql_not_supported(
                "First arg to date_add must be a string literal",
            ));
        };

        let num = if let Expr::Literal(ScalarValue::Int32(Some(num))) = &args[1] {
            *num
        } else {
            return Err(VegaFusionError::sql_not_supported(
                "Second arg to date_add must be an integer",
            ));
        };

        let interval_string = format!("{num} {date_part}");
        let interval = SqlExpr::Interval {
            value: Box::new(SqlExpr::Value(SqlValue::SingleQuotedString(
                interval_string,
            ))),
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        };

        Ok(SqlExpr::BinaryOp {
            left: Box::new(args[2].to_sql(dialect)?),
            op: SqlBinaryOperator::Plus,
            right: Box::new(interval),
        })
    }
}
