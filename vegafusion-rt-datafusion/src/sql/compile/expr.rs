use crate::sql::compile::data_type::ToSqlDataType;
use crate::sql::compile::scalar::ToSqlScalar;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArg, Ident, ObjectName as SqlObjectName, ObjectName,
    UnaryOperator as SqlUnaryOperator, WindowFrame as SqlWindowFrame,
    WindowFrameBound as SqlWindowBound, WindowFrameUnits as SqlWindowFrameUnits,
    WindowSpec as SqlWindowSpec,
};

use datafusion_expr::expr::{BinaryExpr, Case, Cast};
use datafusion_expr::{
    expr, lit, AggregateFunction, Between, BuiltinScalarFunction, Expr, Operator, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};
use vegafusion_core::data::scalar::ScalarValueHelpers;

use crate::sql::compile::function_arg::ToSqlFunctionArg;
use crate::sql::compile::order::ToSqlOrderByExpr;
use crate::sql::dialect::Dialect;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait ToSqlExpr {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlExpr>;
}

impl ToSqlExpr for Expr {
    fn to_sql(&self, dialect: &Dialect) -> Result<SqlExpr> {
        match self {
            Expr::Alias(_, _) => {
                // Alias expressions need to be handled at a higher level
                Err(VegaFusionError::internal(format!(
                    "Alias cannot be converted to SQL: {self:?}"
                )))
            }
            Expr::Column(col) => Ok(match &col.relation {
                Some(relation) => SqlExpr::CompoundIdentifier(vec![
                    Ident::with_quote(dialect.quote_style, relation),
                    Ident::with_quote(dialect.quote_style, &col.name),
                ]),
                None => SqlExpr::Identifier(Ident::with_quote(dialect.quote_style, &col.name)),
            }),
            Expr::ScalarVariable(_, _) => Err(VegaFusionError::internal(
                "ScalarVariable cannot be converted to SQL",
            )),
            Expr::Literal(value) => Ok(value.to_sql(dialect)?),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let sql_op = match op {
                    Operator::Eq => SqlBinaryOperator::Eq,
                    Operator::NotEq => SqlBinaryOperator::NotEq,
                    Operator::Lt => SqlBinaryOperator::Lt,
                    Operator::LtEq => SqlBinaryOperator::LtEq,
                    Operator::Gt => SqlBinaryOperator::Gt,
                    Operator::GtEq => SqlBinaryOperator::GtEq,
                    Operator::Plus => SqlBinaryOperator::Plus,
                    Operator::Minus => SqlBinaryOperator::Minus,
                    Operator::Multiply => SqlBinaryOperator::Multiply,
                    Operator::Divide => SqlBinaryOperator::Divide,
                    Operator::Modulo => SqlBinaryOperator::Modulo,
                    Operator::And => SqlBinaryOperator::And,
                    Operator::Or => SqlBinaryOperator::Or,
                    Operator::IsDistinctFrom => {
                        return Err(VegaFusionError::internal(
                            "IsDistinctFrom cannot be converted to SQL".to_string(),
                        ))
                    }
                    Operator::IsNotDistinctFrom => {
                        return Err(VegaFusionError::internal(
                            "IsNotDistinctFrom cannot be converted to SQL".to_string(),
                        ))
                    }
                    Operator::RegexMatch => SqlBinaryOperator::PGRegexMatch,
                    Operator::RegexIMatch => SqlBinaryOperator::PGRegexIMatch,
                    Operator::RegexNotMatch => SqlBinaryOperator::PGRegexNotMatch,
                    Operator::RegexNotIMatch => SqlBinaryOperator::PGRegexNotIMatch,
                    Operator::BitwiseAnd => SqlBinaryOperator::BitwiseAnd,
                    Operator::BitwiseOr => SqlBinaryOperator::BitwiseOr,
                    Operator::BitwiseXor => SqlBinaryOperator::BitwiseXor,
                    Operator::StringConcat => SqlBinaryOperator::StringConcat,
                    Operator::BitwiseShiftRight => SqlBinaryOperator::PGBitwiseShiftRight,
                    Operator::BitwiseShiftLeft => SqlBinaryOperator::PGBitwiseShiftLeft,
                };
                Ok(SqlExpr::Nested(Box::new(SqlExpr::BinaryOp {
                    left: Box::new(left.to_sql(dialect)?),
                    op: sql_op,
                    right: Box::new(right.to_sql(dialect)?),
                })))
            }
            Expr::Not(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Not,
                expr: Box::new(expr.to_sql(dialect)?),
            }))),
            Expr::IsNotNull(expr) => Ok(SqlExpr::IsNotNull(Box::new(expr.to_sql(dialect)?))),
            Expr::IsNull(expr) => Ok(SqlExpr::IsNull(Box::new(expr.to_sql(dialect)?))),
            Expr::Negative(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Minus,
                expr: Box::new(expr.to_sql(dialect)?),
            }))),
            Expr::GetIndexedField { .. } => Err(VegaFusionError::internal(
                "GetIndexedField cannot be converted to SQL",
            )),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Ok(SqlExpr::Between {
                expr: Box::new(expr.to_sql(dialect)?),
                negated: *negated,
                low: Box::new(low.to_sql(dialect)?),
                high: Box::new(high.to_sql(dialect)?),
            }),
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let (conditions, results): (Vec<Box<Expr>>, Vec<Box<Expr>>) =
                    when_then_expr.iter().cloned().unzip();

                let conditions = conditions
                    .iter()
                    .map(|expr| expr.to_sql(dialect))
                    .collect::<Result<Vec<_>>>()?;
                let results = results
                    .iter()
                    .map(|expr| expr.to_sql(dialect))
                    .collect::<Result<Vec<_>>>()?;

                let else_result = if let Some(else_expr) = &else_expr {
                    Some(Box::new(else_expr.to_sql(dialect)?))
                } else {
                    None
                };

                Ok(SqlExpr::Case {
                    operand: if let Some(expr) = &expr {
                        Some(Box::new(expr.to_sql(dialect)?))
                    } else {
                        None
                    },
                    conditions,
                    results,
                    else_result,
                })
            }
            Expr::Cast(Cast { expr, data_type }) => {
                let data_type = data_type.to_sql(dialect)?;
                Ok(SqlExpr::Cast {
                    expr: Box::new(expr.to_sql(dialect)?),
                    data_type,
                })
            }
            Expr::TryCast(expr::TryCast { expr, data_type }) => {
                let data_type = data_type.to_sql(dialect)?;
                Ok(SqlExpr::TryCast {
                    expr: Box::new(expr.to_sql(dialect)?),
                    data_type,
                })
            }
            Expr::Sort { .. } => {
                // Sort expressions need to be handled at a higher level
                Err(VegaFusionError::internal("Sort cannot be converted to SQL"))
            }
            Expr::ScalarFunction { fun, args } => {
                let fun_name = match fun {
                    BuiltinScalarFunction::Abs => "abs",
                    BuiltinScalarFunction::Acos => "acos",
                    BuiltinScalarFunction::Asin => "asin",
                    BuiltinScalarFunction::Atan => "atan",
                    BuiltinScalarFunction::Atan2 => "atan2",
                    BuiltinScalarFunction::Ceil => "ceil",
                    BuiltinScalarFunction::Coalesce => "coalesce",
                    BuiltinScalarFunction::Cos => "cos",
                    BuiltinScalarFunction::Digest => "digest",
                    BuiltinScalarFunction::Exp => "exp",
                    BuiltinScalarFunction::Floor => "floor",
                    BuiltinScalarFunction::Ln => "ln",
                    BuiltinScalarFunction::Log => "log",
                    BuiltinScalarFunction::Log10 => "log10",
                    BuiltinScalarFunction::Log2 => "log2",
                    BuiltinScalarFunction::Power => "pow",
                    BuiltinScalarFunction::Round => "round",
                    BuiltinScalarFunction::Signum => "signum",
                    BuiltinScalarFunction::Sin => "sin",
                    BuiltinScalarFunction::Sqrt => "sqrt",
                    BuiltinScalarFunction::Tan => "tan",
                    BuiltinScalarFunction::Trunc => "trunc",
                    BuiltinScalarFunction::MakeArray => "make_array",
                    BuiltinScalarFunction::Ascii => "ascii",
                    BuiltinScalarFunction::BitLength => "bit_length",
                    BuiltinScalarFunction::Btrim => "btrim",
                    BuiltinScalarFunction::CharacterLength => "length",
                    BuiltinScalarFunction::Chr => "chr",
                    BuiltinScalarFunction::Concat => "concat",
                    BuiltinScalarFunction::ConcatWithSeparator => "concat_ws",
                    BuiltinScalarFunction::DatePart => "date_part",
                    BuiltinScalarFunction::DateTrunc => "date_trunc",
                    BuiltinScalarFunction::DateBin => "date_bin",
                    BuiltinScalarFunction::InitCap => "initcap",
                    BuiltinScalarFunction::Left => "left",
                    BuiltinScalarFunction::Lpad => "lpad",
                    BuiltinScalarFunction::Lower => "lower",
                    BuiltinScalarFunction::Ltrim => "ltrim",
                    BuiltinScalarFunction::MD5 => "md5",
                    BuiltinScalarFunction::NullIf => "nullif",
                    BuiltinScalarFunction::OctetLength => "octet_length",
                    BuiltinScalarFunction::Random => "random",
                    BuiltinScalarFunction::RegexpReplace => "regexp_replace",
                    BuiltinScalarFunction::Repeat => "repeat",
                    BuiltinScalarFunction::Replace => "replace",
                    BuiltinScalarFunction::Reverse => "reverse",
                    BuiltinScalarFunction::Right => "right",
                    BuiltinScalarFunction::Rpad => "rpad",
                    BuiltinScalarFunction::Rtrim => "rtrim",
                    BuiltinScalarFunction::SHA224 => "sha224",
                    BuiltinScalarFunction::SHA256 => "sha256",
                    BuiltinScalarFunction::SHA384 => "sha384",
                    BuiltinScalarFunction::SHA512 => "sha512",
                    BuiltinScalarFunction::SplitPart => "split_part",
                    BuiltinScalarFunction::StartsWith => "starts_with",
                    BuiltinScalarFunction::Strpos => "strpos",
                    BuiltinScalarFunction::Substr => "substr",
                    BuiltinScalarFunction::ToHex => "to_hex",
                    BuiltinScalarFunction::ToTimestamp => "to_timestamp",
                    BuiltinScalarFunction::ToTimestampMillis => "to_timestamp_millis",
                    BuiltinScalarFunction::ToTimestampMicros => "to_timestamp_micros",
                    BuiltinScalarFunction::ToTimestampSeconds => "to_timestamp_seconds",
                    BuiltinScalarFunction::FromUnixtime => "from_unixtime",
                    BuiltinScalarFunction::Now => "now",
                    BuiltinScalarFunction::Translate => "translate",
                    BuiltinScalarFunction::Trim => "trim",
                    BuiltinScalarFunction::Upper => "upper",
                    BuiltinScalarFunction::RegexpMatch => "regexp_match",
                    BuiltinScalarFunction::Struct => "struct",
                    BuiltinScalarFunction::ArrowTypeof => "arrow_typeof",
                    BuiltinScalarFunction::CurrentDate => "current_date",
                    BuiltinScalarFunction::CurrentTime => "current_time",
                    BuiltinScalarFunction::Uuid => "uuid",
                };
                translate_scalar_function(fun_name, args, dialect)
            }
            Expr::ScalarUDF { fun, args } => translate_scalar_function(&fun.name, args, dialect),
            Expr::AggregateFunction(expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter: _,
            }) => {
                let fun_name = aggr_fn_to_name(fun);
                translate_aggregate_function(fun_name, args, *distinct, dialect)
            }
            Expr::AggregateUDF {
                fun,
                args,
                filter: _,
            } => translate_aggregate_function(&fun.name, args, false, dialect),
            Expr::WindowFunction(expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }) => {
                // Extract function name
                let fun_name = match fun {
                    WindowFunction::AggregateFunction(agg) => aggr_fn_to_name(agg).to_string(),
                    WindowFunction::BuiltInWindowFunction(win_fn) => win_fn.to_string(),
                    WindowFunction::AggregateUDF(udf) => udf.name.clone(),
                }
                .to_ascii_lowercase();

                if dialect.aggregate_functions.contains(&fun_name)
                    || dialect.window_functions.contains(&fun_name)
                {
                    // Process args
                    let args = translate_function_args(args, dialect)?;

                    let partition_by = partition_by
                        .iter()
                        .map(|arg| arg.to_sql(dialect))
                        .collect::<Result<Vec<_>>>()?;

                    let order_by = order_by
                        .iter()
                        .map(|arg| arg.to_sql_order(dialect))
                        .collect::<Result<Vec<_>>>()?;

                    let end_bound = compile_window_frame_bound(&window_frame.end_bound, dialect)?;
                    let start_bound =
                        compile_window_frame_bound(&window_frame.start_bound, dialect)?;
                    let units = match window_frame.units {
                        WindowFrameUnits::Rows => SqlWindowFrameUnits::Rows,
                        WindowFrameUnits::Range => SqlWindowFrameUnits::Range,
                        WindowFrameUnits::Groups => SqlWindowFrameUnits::Groups,
                    };
                    let sql_window_frame = Some(SqlWindowFrame {
                        units,
                        start_bound,
                        end_bound: Some(end_bound),
                    });

                    // Process over
                    let over = SqlWindowSpec {
                        partition_by,
                        order_by,
                        window_frame: sql_window_frame,
                    };

                    let sql_fun = SqlFunction {
                        name: ObjectName(vec![Ident {
                            value: fun_name,
                            quote_style: None,
                        }]),
                        args,
                        over: Some(over),
                        distinct: false,
                        special: false,
                    };

                    Ok(SqlExpr::Function(sql_fun))
                } else {
                    // Unsupported
                    return Err(VegaFusionError::sql_not_supported(format!(
                        "Dialect does not support the '{fun_name}' window function"
                    )));
                }
            }
            Expr::IsTrue(_) => Err(VegaFusionError::internal(
                "IsTrue cannot be converted to SQL",
            )),
            Expr::IsFalse(_) => Err(VegaFusionError::internal(
                "IsFalse cannot be converted to SQL",
            )),
            Expr::IsUnknown(_) => Err(VegaFusionError::internal(
                "IsUnknown cannot be converted to SQL",
            )),
            Expr::IsNotTrue(_) => Err(VegaFusionError::internal(
                "IsNotTrue cannot be converted to SQL",
            )),
            Expr::IsNotFalse(_) => Err(VegaFusionError::internal(
                "IsNotFalse cannot be converted to SQL",
            )),
            Expr::IsNotUnknown(_) => Err(VegaFusionError::internal(
                "IsNotUnknown cannot be converted to SQL",
            )),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let sql_expr = expr.to_sql(dialect)?;
                let sql_list = list
                    .iter()
                    .map(|expr| expr.to_sql(dialect))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::InList {
                    expr: Box::new(sql_expr),
                    list: sql_list,
                    negated: *negated,
                })
            }
            Expr::Wildcard => Err(VegaFusionError::internal(
                "Wildcard cannot be converted to SQL",
            )),
            Expr::Exists { .. } => Err(VegaFusionError::internal(
                "Exists cannot be converted to SQL",
            )),
            Expr::InSubquery { .. } => Err(VegaFusionError::internal(
                "InSubquery cannot be converted to SQL",
            )),
            Expr::ScalarSubquery(_) => Err(VegaFusionError::internal(
                "ScalarSubquery cannot be converted to SQL",
            )),
            Expr::QualifiedWildcard { .. } => Err(VegaFusionError::internal(
                "QualifiedWildcard cannot be converted to SQL",
            )),
            Expr::GroupingSet(_) => Err(VegaFusionError::internal(
                "GroupingSet cannot be converted to SQL",
            )),
            Expr::Like { .. } => Err(VegaFusionError::internal("Like cannot be converted to SQL")),
            Expr::ILike { .. } => Err(VegaFusionError::internal(
                "ILike cannot be converted to SQL",
            )),
            Expr::SimilarTo { .. } => Err(VegaFusionError::internal(
                "SimilarTo cannot be converted to SQL",
            )),
            Expr::Placeholder { .. } => Err(VegaFusionError::internal(
                "Placeholder cannot be converted to SQL",
            )),
        }
    }
}

fn translate_scalar_function(
    fun_name: &str,
    args: &Vec<Expr>,
    dialect: &Dialect,
) -> Result<SqlExpr> {
    if dialect.scalar_functions.contains(fun_name) {
        // Function is directly supported by dialect
        let ident = Ident {
            value: fun_name.to_string(),
            quote_style: None,
        };
        let args = translate_function_args(args, dialect)?;

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![ident]),
            args,
            over: None,
            distinct: false,
            special: false,
        }))
    } else if let Some(transformer) = dialect.scalar_transformers.get(fun_name) {
        // Supported through AST transformation
        transformer.transform(args, dialect)
    } else {
        // Unsupported
        return Err(VegaFusionError::sql_not_supported(format!(
            "Dialect does not support the '{fun_name}' scalar function"
        )));
    }
}

fn translate_aggregate_function(
    fun_name: &str,
    args: &Vec<Expr>,
    distinct: bool,
    dialect: &Dialect,
) -> Result<SqlExpr> {
    if dialect.aggregate_functions.contains(fun_name) {
        let ident = Ident {
            value: fun_name.to_ascii_lowercase(),
            quote_style: None,
        };
        let args = translate_function_args(args, dialect)?;

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![ident]),
            args,
            over: None,
            distinct,
            special: false,
        }))
    } else if let Some(transformer) = dialect.aggregate_transformers.get(fun_name) {
        // Supported through AST transformation
        transformer.transform(args, dialect)
    } else {
        // Unsupported
        return Err(VegaFusionError::sql_not_supported(format!(
            "Dialect does not support the '{fun_name}' aggregate function"
        )));
    }
}

fn translate_function_args(args: &Vec<Expr>, dialect: &Dialect) -> Result<Vec<FunctionArg>> {
    args.iter()
        .map(|expr| Ok(SqlFunctionArg::Unnamed(expr.to_sql_function_arg(dialect)?)))
        .collect::<Result<Vec<_>>>()
}

fn aggr_fn_to_name(fun: &AggregateFunction) -> &str {
    match fun {
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
        AggregateFunction::Count => "count",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Median => "median",
        AggregateFunction::ApproxDistinct => "approx_distinct",
        AggregateFunction::ArrayAgg => "array_agg",
        AggregateFunction::Variance => "var",
        AggregateFunction::VariancePop => "var_pop",
        AggregateFunction::Stddev => "stddev",
        AggregateFunction::StddevPop => "stddev_pop",
        AggregateFunction::Covariance => "covar",
        AggregateFunction::CovariancePop => "covar_pop",
        AggregateFunction::Correlation => "corr",
        AggregateFunction::ApproxPercentileCont => "approx_percentile_cont",
        AggregateFunction::ApproxPercentileContWithWeight => "approx_percentile_cont_with_weight",
        AggregateFunction::ApproxMedian => "approx_median",
        AggregateFunction::Grouping => "grouping",
    }
}

fn compile_window_frame_bound(
    bound: &WindowFrameBound,
    dialect: &Dialect,
) -> Result<SqlWindowBound> {
    Ok(match bound {
        WindowFrameBound::Preceding(v) => match v.to_f64() {
            Ok(v) => {
                SqlWindowBound::Preceding(Some(Box::new(lit(v.max(0.0) as u64).to_sql(dialect)?)))
            }
            Err(_) => SqlWindowBound::Preceding(None),
        },
        WindowFrameBound::CurrentRow => SqlWindowBound::CurrentRow,
        WindowFrameBound::Following(v) => match v.to_f64() {
            Ok(v) => {
                SqlWindowBound::Following(Some(Box::new(lit(v.max(0.0) as u64).to_sql(dialect)?)))
            }
            Err(_) => SqlWindowBound::Following(None),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::ToSqlExpr;
    use crate::expression::escape::flat_col;
    use crate::sql::connection::datafusion_conn::make_datafusion_dialect;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{lit, Between, BuiltinScalarFunction, Expr};
    use vegafusion_core::arrow::datatypes::DataType;

    #[test]
    pub fn test1() {
        let df_expr = Expr::Negative(Box::new(flat_col("A"))) + lit(12);
        let sql_expr = df_expr.to_sql(&Default::default()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"((-"A") + 12)"#.to_string());
    }

    #[test]
    pub fn test2() {
        let df_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Sin,
            args: vec![lit(1.2)],
        } + flat_col("B");

        let sql_expr = df_expr.to_sql(&make_datafusion_dialect()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"(sin(1.2) + "B")"#.to_string());
    }

    #[test]
    pub fn test3() {
        let df_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Upper,
            args: vec![lit("foo")],
        };

        let sql_expr = df_expr.to_sql(&make_datafusion_dialect()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "upper('foo')".to_string());
    }

    #[test]
    pub fn test4() {
        let df_expr = Expr::Cast(Cast {
            expr: Box::new(lit(2.8)),
            data_type: DataType::Int64,
        }) + lit(4);

        let sql_expr = df_expr.to_sql(&Default::default()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(CAST(2.8 AS BIGINT) + 4)".to_string());
    }

    #[test]
    pub fn test5() {
        let df_expr = Expr::Between(Between {
            expr: Box::new(flat_col("A")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        })
        .or(flat_col("B"));

        let sql_expr = df_expr.to_sql(&Default::default()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"("A" BETWEEN 0 AND 10 OR "B")"#.to_string());
    }
}
