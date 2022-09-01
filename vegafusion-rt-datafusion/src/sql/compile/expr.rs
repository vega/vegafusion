use crate::sql::compile::data_type::ToSqlDataType;
use crate::sql::compile::scalar::ToSqlScalar;
use sqlgen::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Function as SqlFunction,
    FunctionArg as SqlFunctionArg, FunctionArg, FunctionArgExpr as SqlFunctionArgExpr,
    FunctionArgExpr, Ident, ObjectName as SqlObjectName, ObjectName,
    UnaryOperator as SqlUnaryOperator, WindowSpec as SqlWindowSpec,
};

use datafusion_expr::{Expr, Operator, WindowFunction};

use crate::sql::compile::function_arg::ToSqlFunctionArg;
use crate::sql::compile::order::ToSqlOrderByExpr;
use vegafusion_core::error::{Result, VegaFusionError};

pub trait ToSqlExpr {
    fn to_sql(&self) -> Result<SqlExpr>;
}

impl ToSqlExpr for Expr {
    fn to_sql(&self) -> Result<SqlExpr> {
        match self {
            Expr::Alias(_, _) => {
                // Alias expressions need to be handled at a higher level
                Err(VegaFusionError::internal(
                    "Alias cannot be converted to SQL",
                ))
            }
            Expr::Column(col) => Ok(match &col.relation {
                Some(relation) => {
                    SqlExpr::CompoundIdentifier(vec![Ident::new(relation), Ident::new(&col.name)])
                }
                None => SqlExpr::Identifier(Ident::new(&col.name)),
            }),
            Expr::ScalarVariable(_, _) => Err(VegaFusionError::internal(
                "ScalarVariable cannot be converted to SQL",
            )),
            Expr::Literal(value) => Ok(value.to_sql()?),
            Expr::BinaryExpr { left, op, right } => {
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
                    Operator::Like => SqlBinaryOperator::Like,
                    Operator::NotLike => SqlBinaryOperator::NotLike,
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
                    Operator::StringConcat => SqlBinaryOperator::StringConcat,
                    Operator::BitwiseShiftRight => SqlBinaryOperator::PGBitwiseShiftRight,
                    Operator::BitwiseShiftLeft => SqlBinaryOperator::PGBitwiseShiftLeft,
                };
                Ok(SqlExpr::Nested(Box::new(SqlExpr::BinaryOp {
                    left: Box::new(left.to_sql()?),
                    op: sql_op,
                    right: Box::new(right.to_sql()?),
                })))
            }
            Expr::Not(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Not,
                expr: Box::new(expr.to_sql()?),
            }))),
            Expr::IsNotNull(expr) => Ok(SqlExpr::IsNotNull(Box::new(expr.to_sql()?))),
            Expr::IsNull(expr) => Ok(SqlExpr::IsNull(Box::new(expr.to_sql()?))),
            Expr::Negative(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Minus,
                expr: Box::new(expr.to_sql()?),
            }))),
            Expr::GetIndexedField { .. } => Err(VegaFusionError::internal(
                "GetIndexedField cannot be converted to SQL",
            )),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(SqlExpr::Between {
                expr: Box::new(expr.to_sql()?),
                negated: *negated,
                low: Box::new(low.to_sql()?),
                high: Box::new(high.to_sql()?),
            }),
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let (conditions, results): (Vec<Box<Expr>>, Vec<Box<Expr>>) =
                    when_then_expr.iter().cloned().unzip();

                let conditions = conditions
                    .iter()
                    .map(|expr| expr.to_sql())
                    .collect::<Result<Vec<_>>>()?;
                let results = results
                    .iter()
                    .map(|expr| expr.to_sql())
                    .collect::<Result<Vec<_>>>()?;

                let else_result = if let Some(else_expr) = &else_expr {
                    Some(Box::new(else_expr.to_sql()?))
                } else {
                    None
                };

                Ok(SqlExpr::Case {
                    operand: if let Some(expr) = &expr {
                        Some(Box::new(expr.to_sql()?))
                    } else {
                        None
                    },
                    conditions,
                    results,
                    else_result,
                })
            }
            Expr::Cast { expr, data_type } => {
                let data_type = data_type.to_sql()?;
                Ok(SqlExpr::Cast {
                    expr: Box::new(expr.to_sql()?),
                    data_type,
                })
            }
            Expr::TryCast { expr, data_type } => {
                let data_type = data_type.to_sql()?;
                Ok(SqlExpr::TryCast {
                    expr: Box::new(expr.to_sql()?),
                    data_type,
                })
            }
            Expr::Sort { .. } => {
                // Sort expressions need to be handled at a higher level
                Err(VegaFusionError::internal("Sort cannot be converted to SQL"))
            }
            Expr::ScalarFunction { fun, args } => {
                let ident = Ident {
                    value: fun.to_string(),
                    quote_style: None,
                };
                let args = args
                    .iter()
                    .map(|expr| Ok(SqlFunctionArg::Unnamed(expr.to_sql_function_arg()?)))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![ident]),
                    args,
                    over: None,
                    distinct: false,
                }))
            }
            Expr::ScalarUDF { fun, args } => {
                let ident = Ident {
                    value: fun.name.clone(),
                    quote_style: None,
                };
                let args = args
                    .iter()
                    .map(|expr| Ok(SqlFunctionArg::Unnamed(expr.to_sql_function_arg()?)))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![ident]),
                    args,
                    over: None,
                    distinct: false,
                }))
            }
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => {
                let ident = Ident {
                    value: fun.to_string(),
                    quote_style: None,
                };
                let args = args
                    .iter()
                    .map(|expr| Ok(SqlFunctionArg::Unnamed(expr.to_sql_function_arg()?)))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![ident]),
                    args,
                    over: None,
                    distinct: *distinct,
                }))
            }
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                // Extract function name
                let name_str = match fun {
                    WindowFunction::AggregateFunction(agg) => agg.to_string(),
                    WindowFunction::BuiltInWindowFunction(win_fn) => win_fn.to_string(),
                };

                // Process args
                let args = args
                    .iter()
                    .map(|arg| Ok(SqlFunctionArg::Unnamed(arg.to_sql_function_arg()?)))
                    .collect::<Result<Vec<_>>>()?;

                let partition_by = partition_by
                    .iter()
                    .map(|arg| arg.to_sql())
                    .collect::<Result<Vec<_>>>()?;

                let order_by = order_by
                    .iter()
                    .map(|arg| arg.to_sql_order())
                    .collect::<Result<Vec<_>>>()?;

                if window_frame.is_some() {
                    return Err(VegaFusionError::internal(
                        "Window frame is not yet supported",
                    ));
                }

                // Process over
                let over = SqlWindowSpec {
                    partition_by,
                    order_by,
                    window_frame: None,
                };

                let sql_fun = SqlFunction {
                    name: ObjectName(vec![Ident {
                        value: name_str,
                        quote_style: None,
                    }]),
                    args,
                    over: Some(over),
                    distinct: false,
                };

                Ok(SqlExpr::Function(sql_fun))
            }
            Expr::AggregateUDF { fun, args } => {
                let ident = Ident {
                    value: fun.name.clone(),
                    quote_style: None,
                };
                let args = args
                    .iter()
                    .map(|expr| Ok(SqlFunctionArg::Unnamed(expr.to_sql_function_arg()?)))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![ident]),
                    args,
                    over: None,
                    distinct: false,
                }))
            }
            Expr::InList { .. } => Err(VegaFusionError::internal(
                "InList cannot be converted to SQL",
            )),
            Expr::Wildcard => Err(VegaFusionError::internal(
                "Wildcard cannot be converted to SQL",
            )),
            Expr::Exists { .. } => Err(VegaFusionError::internal(
                "Exists cannot be converted to SQL",
            )),
            Expr::InSubquery { .. } => Err(VegaFusionError::internal(
                "InSubquery  cannot be converted to SQL",
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ToSqlExpr;
    use datafusion_expr::{col, lit, BuiltinScalarFunction, Expr};
    use sqlgen::dialect::DialectDisplay;
    use vegafusion_core::arrow::datatypes::DataType;

    #[test]
    pub fn test1() {
        let df_expr = Expr::Negative(Box::new(col("A"))) + lit(12);
        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "- A + 12".to_string());
    }

    #[test]
    pub fn test2() {
        let df_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Sin,
            args: vec![lit(1.2)],
        } + col("B");

        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "sin(1.2) + B".to_string());
    }

    #[test]
    pub fn test3() {
        let df_expr = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Upper,
            args: vec![lit("foo")],
        };

        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "upper(\"foo\")".to_string());
    }

    #[test]
    pub fn test4() {
        let df_expr = Expr::Cast {
            expr: Box::new(lit(2.8)),
            data_type: DataType::Int64,
        } + lit(4);

        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "CAST(2.8 AS INT) + 4".to_string());
    }

    #[test]
    pub fn test5() {
        let df_expr = Expr::Between {
            expr: Box::new(col("A")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        }
        .or(col("B"));

        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "A BETWEEN 0 AND 10 OR B".to_string());
    }
}
