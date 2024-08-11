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
            Expr::Wildcard { qualifier: None } => SqlFunctionArgExpr::Wildcard,
            Expr::Wildcard {
                qualifier: Some(qualifier),
            } => SqlFunctionArgExpr::QualifiedWildcard(ObjectName(
                qualifier
                    .to_vec()
                    .into_iter()
                    .map(|value| Ident {
                        value,
                        quote_style: None,
                    })
                    .collect(),
            )),
            expr => SqlFunctionArgExpr::Expr(expr.to_sql(dialect, schema)?),
        })
    }
}
