use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{FunctionArgExpr as SqlFunctionArgExpr, Ident, ObjectName};
use vegafusion_common::error::Result;

pub trait ToSqlFunctionArg {
    fn to_sql_function_arg(
        &self,
        dialect: &Dialect,
        schema: &DFSchema,
    ) -> Result<SqlFunctionArgExpr>;
}

impl ToSqlFunctionArg for Expr {
    fn to_sql_function_arg(
        &self,
        dialect: &Dialect,
        schema: &DFSchema,
    ) -> Result<SqlFunctionArgExpr> {
        Ok(match self {
            Expr::Wildcard => SqlFunctionArgExpr::Wildcard,
            Expr::QualifiedWildcard { qualifier } => {
                SqlFunctionArgExpr::QualifiedWildcard(ObjectName(vec![Ident {
                    value: qualifier.clone(),
                    quote_style: None,
                }]))
            }
            expr => SqlFunctionArgExpr::Expr(expr.to_sql(dialect, schema)?),
        })
    }
}
