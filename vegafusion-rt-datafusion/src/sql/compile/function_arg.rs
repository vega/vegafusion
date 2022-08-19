use crate::sql::compile::expr::ToSqlExpr;
use datafusion_expr::Expr;
use sqlgen::ast::{Ident, FunctionArgExpr as SqlFunctionArgExpr, ObjectName};
use vegafusion_core::error::Result;

pub trait ToSqlFunctionArg {
    fn to_sql_function_arg(&self) -> Result<SqlFunctionArgExpr>;
}

impl ToSqlFunctionArg for Expr {
    fn to_sql_function_arg(&self) -> Result<SqlFunctionArgExpr> {
        Ok(match self {
            Expr::Wildcard => SqlFunctionArgExpr::Wildcard,
            Expr::QualifiedWildcard { qualifier } => SqlFunctionArgExpr::QualifiedWildcard(ObjectName(vec![Ident{ value: qualifier.clone(), quote_style: None }])),
            expr => SqlFunctionArgExpr::Expr(expr.to_sql()?),
        })
    }
}
