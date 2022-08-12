use crate::ast::{
    expr::Expr as SqlExpr,
    function::{
        Function as SqlFunction, FunctionArg as SqlFunctionArg,
        FunctionArgExpr as SqlFunctionArgExpr,
    },
    ident::{Ident, ObjectName as SqlObjectName},
    operator::{BinaryOperator as SqlBinaryOperator, UnaryOperator as SqlUnaryOperator},
};
use crate::compile::data_type::ToSqlDataType;
use crate::compile::scalar::ToSqlScalar;

use datafusion_expr::{Expr, Operator};

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
            Expr::Literal(value) => Ok(SqlExpr::Value(value.to_sql()?)),
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
                };
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(left.to_sql()?),
                    op: sql_op,
                    right: Box::new(right.to_sql()?),
                })
            }
            Expr::Not(expr) => Ok(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Not,
                expr: Box::new(expr.to_sql()?),
            }),
            Expr::IsNotNull(expr) => Ok(SqlExpr::IsNull(Box::new(expr.to_sql()?))),
            Expr::IsNull(expr) => Ok(SqlExpr::IsNotNull(Box::new(expr.to_sql()?))),
            Expr::Negative(expr) => Ok(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Minus,
                expr: Box::new(expr.to_sql()?),
            }),
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
                    .map(|expr| {
                        Ok(SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                            expr.to_sql()?,
                        )))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::Function(SqlFunction {
                    name: SqlObjectName(vec![ident]),
                    args,
                    over: None,
                    distinct: false,
                }))
            }
            Expr::ScalarUDF { .. } => Err(VegaFusionError::internal(
                "ScalarUDF cannot be converted to SQL",
            )),
            Expr::AggregateFunction { .. } => Err(VegaFusionError::internal(
                "AggregateFunction cannot be converted to SQL",
            )),
            Expr::WindowFunction { .. } => Err(VegaFusionError::internal(
                "WindowFunction cannot be converted to SQL",
            )),
            Expr::AggregateUDF { fun, args } => {
                let ident = Ident {
                    value: fun.name.clone(),
                    quote_style: None,
                };
                let args = args
                    .iter()
                    .map(|expr| {
                        Ok(SqlFunctionArg::Unnamed(SqlFunctionArgExpr::Expr(
                            expr.to_sql()?,
                        )))
                    })
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
    use crate::ast::display::DialectDisplay;
    use datafusion_expr::{col, lit, BuiltinScalarFunction, Expr};
    use vegafusion_core::arrow::datatypes::DataType;

    #[test]
    pub fn test1() {
        let df_expr = Expr::Negative(Box::new(col("A"))) + lit(12);
        let sql_expr = df_expr.to_sql().unwrap();
        println!("{:?}", sql_expr);
        let sql_str = sql_expr.try_to_string(&Default::default()).unwrap();
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
        let sql_str = sql_expr.try_to_string(&Default::default()).unwrap();
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
        let sql_str = sql_expr.try_to_string(&Default::default()).unwrap();
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
        let sql_str = sql_expr.try_to_string(&Default::default()).unwrap();
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
        let sql_str = sql_expr.try_to_string(&Default::default()).unwrap();
        assert_eq!(sql_str, "A BETWEEN 0 AND 10 OR B".to_string());
    }
}