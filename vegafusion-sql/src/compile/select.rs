use datafusion_expr::Expr;
use sqlgen::ast::{Ident, SelectItem as SqlSelectItem};
use vegafusion_core::error::Result;
use crate::compile::expr::ToSqlExpr;


pub trait ToSqlSelectItem {
    fn to_sql_select(&self) -> Result<SqlSelectItem>;
}

impl ToSqlSelectItem for Expr {
    fn to_sql_select(&self) -> Result<SqlSelectItem> {
        Ok(match self {
            Expr::Alias(expr, alias) => {
                SqlSelectItem::ExprWithAlias { expr: expr.to_sql()?, alias: Ident { value: alias.clone(), quote_style: Some('"')} }
            }
            Expr::Wildcard => {
                SqlSelectItem::Wildcard
            }
            expr => {
                SqlSelectItem::UnnamedExpr(expr.to_sql()?)
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use std::ops::Add;
    use super::ToSqlExpr;
    use sqlgen::dialect::{Dialect, DialectDisplay};
    use datafusion_expr::{col, lit, BuiltinScalarFunction, Expr};
    use vegafusion_core::arrow::datatypes::DataType;
    use crate::compile::order::ToSqlOrderByExpr;
    use crate::compile::select::ToSqlSelectItem;

    #[test]
    pub fn test_select_wildcard() {
        let expr = Expr::Wildcard;
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "*");
    }

    #[test]
    pub fn test_select_unnamed_expr() {
        let expr = col("a").add(lit(23));
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "\"a\" + 23");
    }

    #[test]
    pub fn test_select_aliased_expr() {
        let expr = col("a").add(lit(23)).alias("foo");
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "\"a\" + 23 AS \"foo\"");
    }
}