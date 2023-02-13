use crate::compile::expr::ToSqlExpr;
use datafusion_common::scalar::ScalarValue;
use datafusion_expr::{Expr, Operator};
use sqlparser::ast::{BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function as SqlFunction, Function, FunctionArg as SqlFunctionArg, FunctionArg, FunctionArgExpr as SqlFunctionArgExpr, FunctionArgExpr, Ident as SqlIdent, Ident, ObjectName as SqlObjectName, ObjectName, Value as SqlValue};
use sqlparser::dialect::{
    BigQueryDialect, ClickHouseDialect, Dialect as SqlParserDialect, GenericDialect, MySqlDialect,
    PostgreSqlDialect, RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect,
};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};

#[derive(Clone, Debug)]
pub enum ParseDialect {
    Athena,
    BigQuery,
    ClickHouse,
    Databricks,
    DataFusion,
    Dremio,
    DuckDB,
    Generic,
    MySql,
    Postgres,
    Redshift,
    Snowflake,
    SqLite,
}

impl ParseDialect {
    pub fn parser_dialect(&self) -> Arc<dyn SqlParserDialect> {
        match self {
            ParseDialect::Athena => Arc::new(GenericDialect),
            ParseDialect::BigQuery => Arc::new(BigQueryDialect),
            ParseDialect::ClickHouse => Arc::new(ClickHouseDialect {}),
            ParseDialect::Databricks => {
                // sqlparser-rs doesn't have a Databricks dialect. Use MySql since the backtick
                // quoted identifier syntax matches
                Arc::new(MySqlDialect {})
            }
            ParseDialect::DataFusion => Arc::new(GenericDialect),
            ParseDialect::Dremio => Arc::new(GenericDialect),
            ParseDialect::DuckDB => Arc::new(GenericDialect),
            ParseDialect::Generic => Arc::new(GenericDialect),
            ParseDialect::MySql => Arc::new(MySqlDialect {}),
            ParseDialect::Postgres => Arc::new(PostgreSqlDialect {}),
            ParseDialect::Redshift => Arc::new(RedshiftSqlDialect {}),
            ParseDialect::Snowflake => Arc::new(SnowflakeDialect),
            ParseDialect::SqLite => Arc::new(SQLiteDialect {}),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ValuesMode {
    /// SELECT * FROM (VALUES (1, 2) (3, 4)) as _table(a, b)
    ValuesWithSubqueryColumnAliases { explicit_row: bool },

    /// SELECT column1 as a, column2 as b FROM (VALUES (1, 2) (3, 4))
    ValuesWithSelectColumnAliases {
        explicit_row: bool,
        column_prefix: String,
        base_index: usize,
    },

    /// SELECT 1 as a, 2 as b UNION ALL SELECT 3 as a, 4 as b
    SelectUnion,
}

#[derive(Clone, Debug)]
pub struct Dialect {
    /// sqlparser dialect to use to parse queries
    parse_dialect: ParseDialect,

    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub quote_style: char,

    /// Supported binary operators
    pub binary_ops: HashSet<Operator>,

    /// Transforms for binary operators
    pub binary_op_transforms: HashMap<Operator, Arc<dyn BinaryOperatorTransformer>>,

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

    /// Implementation mode for inline VALUES
    pub values_mode: ValuesMode,

    /// Whether NULLS FIRST, NULLS LAST is supported in ORDER BY
    pub supports_null_ordering: bool,
}

impl Default for Dialect {
    fn default() -> Self {
        Self {
            parse_dialect: ParseDialect::Generic,
            quote_style: '"',
            binary_ops: Default::default(),
            binary_op_transforms: Default::default(),
            scalar_functions: Default::default(),
            aggregate_functions: Default::default(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers: Default::default(),
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }
}

impl Dialect {
    pub fn parser_dialect(&self) -> Arc<dyn SqlParserDialect> {
        self.parse_dialect.parser_dialect()
    }

    pub fn athena() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
            ("covar", RenameFunctionTransformer::new_dyn("covar_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::Athena,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn bigquery() -> Self {
        use Operator::*;
        Self {
            parse_dialect: ParseDialect::BigQuery,
            quote_style: '`',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: vec![(
                Modulo,
                Arc::new(ModulusOpToFunction) as Arc<dyn BinaryOperatorTransformer>,
            )]
                .into_iter()
                .collect(),
            scalar_functions: vec![
                "round",
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
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers: Default::default(),
            values_mode: ValuesMode::SelectUnion,
            supports_null_ordering: true,
        }
    }

    pub fn clickhouse() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("varSamp")),
            ("var_pop", RenameFunctionTransformer::new_dyn("varPop")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddevSamp")),
            ("stddev_pop", RenameFunctionTransformer::new_dyn("stddevPop")),
            ("covar", RenameFunctionTransformer::new_dyn("covarSamp")),
            ("covar_pop", RenameFunctionTransformer::new_dyn("covarPop")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::ClickHouse,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::SelectUnion,
            supports_null_ordering: true,
        }
    }

    pub fn databricks() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
            ("covar", RenameFunctionTransformer::new_dyn("covar_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::Databricks,
            quote_style: '`',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn datafusion() -> Self {
        use Operator::*;
        let mut scalar_transforms: HashMap<String, Arc<dyn FunctionTransformer>> = HashMap::new();
        scalar_transforms.insert("date_add".to_string(), Arc::new(DateAddToIntervalAddition));

        Self {
            parse_dialect: ParseDialect::DataFusion,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
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
                "var",
                "var_pop",
                "stddev",
                "stddev_pop",
                "covar",
                "covar_pop",
                "corr"
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
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn dremio() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
            ("covar", RenameFunctionTransformer::new_dyn("covar_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::Dremio,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn duckdb() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::DuckDB,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn mysql() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();
        Self {
            parse_dialect: ParseDialect::MySql,
            quote_style: '`',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases { explicit_row: true },
            supports_null_ordering: false,
        }
    }

    pub fn postgres() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
            ("covar", RenameFunctionTransformer::new_dyn("covar_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();
        Self {
            parse_dialect: ParseDialect::Postgres,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSubqueryColumnAliases {
                explicit_row: false,
            },
            supports_null_ordering: true,
        }
    }

    pub fn redshift() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::Redshift,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                // Median is kind of supported, but usage results in error:
                // "One or more of the used functions must be applied on at least one user created tables"
                "var_pop",
                "stddev_pop",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::SelectUnion,
            supports_null_ordering: true,
        }
    }

    pub fn snowflake() -> Self {
        use Operator::*;
        let aggregate_transformers: HashMap<String, Arc<dyn FunctionTransformer>> = vec![
            ("var", RenameFunctionTransformer::new_dyn("var_samp")),
            ("stddev", RenameFunctionTransformer::new_dyn("stddev_samp")),
            ("covar", RenameFunctionTransformer::new_dyn("covar_samp")),
        ].into_iter().map(
            |(name, v)| (name.to_string(), v)
        ).collect();

        Self {
            parse_dialect: ParseDialect::Snowflake,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
                .into_iter()
                .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
                "var_pop",
                "stddev_pop",
                "covar_pop",
                "corr",
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers,
            values_mode: ValuesMode::ValuesWithSelectColumnAliases {
                explicit_row: false,
                column_prefix: "COLUMN".to_string(),
                base_index: 1,
            },
            supports_null_ordering: true,
        }
    }

    pub fn sqlite() -> Self {
        use Operator::*;
        Self {
            parse_dialect: ParseDialect::SqLite,
            quote_style: '"',
            binary_ops: vec![
                Eq, NotEq, Lt, LtEq, Gt, GtEq, Plus, Minus, Multiply, Divide, Modulo, And, Or,
            ]
            .into_iter()
            .collect(),
            binary_op_transforms: Default::default(),
            scalar_functions: vec![
                "round",
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
            ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            window_functions: Default::default(),
            scalar_transformers: Default::default(),
            aggregate_transformers: Default::default(),
            values_mode: ValuesMode::ValuesWithSelectColumnAliases {
                explicit_row: false,
                column_prefix: "column".to_string(),
                base_index: 1,
            },
            supports_null_ordering: true,
        }
    }
}

impl FromStr for Dialect {
    type Err = VegaFusionError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "athena" => Dialect::athena(),
            "bigquery" => Dialect::bigquery(),
            "clickhouse" => Dialect::clickhouse(),
            "databricks" => Dialect::databricks(),
            "datafusion" => Dialect::datafusion(),
            "dremio" => Dialect::dremio(),
            "duckdb" => Dialect::duckdb(),
            "generic" | "default" => Dialect::default(),
            "mysql" => Dialect::mysql(),
            "postgres" => Dialect::postgres(),
            "redshift" => Dialect::redshift(),
            "snowflake" => Dialect::snowflake(),
            "sqlite" => Dialect::sqlite(),
            _ => {
                return Err(VegaFusionError::sql_not_supported(format!(
                    "Unsupported dialect: {s}"
                )))
            }
        })
    }
}

pub trait BinaryOperatorTransformer: Debug + Send + Sync {
    fn transform(
        &self,
        op: &Operator,
        lhs: SqlExpr,
        rhs: SqlExpr,
        dialect: &Dialect,
    ) -> Result<SqlExpr>;
}

#[derive(Clone, Debug)]
struct ModulusOpToFunction;

impl BinaryOperatorTransformer for ModulusOpToFunction {
    fn transform(
        &self,
        _op: &Operator,
        lhs: SqlExpr,
        rhs: SqlExpr,
        _dialect: &Dialect,
    ) -> Result<SqlExpr> {
        let arg0 = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(lhs));
        let arg1 = SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(rhs));

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![SqlIdent {
                value: "MOD".to_string(),
                quote_style: None,
            }]),
            args: vec![arg0, arg1],
            over: None,
            distinct: false,
            special: false,
        }))
    }
}

pub trait FunctionTransformer: Debug + Send + Sync {
    fn transform(&self, args: &[Expr], dialect: &Dialect) -> Result<SqlExpr>;
}

#[derive(Clone, Debug)]
struct RenameFunctionTransformer(pub String);
impl RenameFunctionTransformer {
    pub fn new_dyn(name: &str) -> Arc<dyn FunctionTransformer> {
        Arc::new(Self(name.to_string()))
    }
}
impl FunctionTransformer for RenameFunctionTransformer {
    fn transform(&self, args: &[Expr], dialect: &Dialect) -> Result<SqlExpr> {
        let sql_args = args.iter().map(|arg| {
            Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg.to_sql(dialect)?)))
        }).collect::<Result<Vec<_>>>()?;

        Ok(SqlExpr::Function(Function {
            name: ObjectName(vec![Ident { value: self.0.clone(), quote_style: None }]),
            args: sql_args,
            over: None,
            distinct: false,
            special: false,
        }))
    }
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
