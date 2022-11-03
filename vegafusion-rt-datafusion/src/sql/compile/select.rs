use crate::sql::compile::expr::ToSqlExpr;
use datafusion_expr::Expr;
use sqlgen::ast::{Ident, SelectItem as SqlSelectItem};
use vegafusion_core::error::Result;

pub trait ToSqlSelectItem {
    fn to_sql_select(&self) -> Result<SqlSelectItem>;
}

impl ToSqlSelectItem for Expr {
    fn to_sql_select(&self) -> Result<SqlSelectItem> {
        Ok(match self {
            Expr::Alias(expr, alias) => SqlSelectItem::ExprWithAlias {
                expr: expr.to_sql()?,
                alias: Ident {
                    value: alias.clone(),
                    quote_style: Some('"'),
                },
            },
            Expr::Wildcard => SqlSelectItem::Wildcard,
            expr => SqlSelectItem::UnnamedExpr(expr.to_sql()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use crate::expression::escape::flat_col;
    use datafusion_expr::{lit, Expr};
    use sqlgen::dialect::{Dialect, DialectDisplay};

    use crate::sql::compile::select::ToSqlSelectItem;

    #[test]
    pub fn test_select_wildcard() {
        let expr = Expr::Wildcard;
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "*");
    }

    #[test]
    pub fn test_select_unnamed_expr() {
        let expr = flat_col("a").add(lit(23));
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "(\"a\" + 23)");
    }

    #[test]
    pub fn test_select_aliased_expr() {
        let expr = flat_col("a").add(lit(23)).alias("foo");
        let sql_expr = expr.to_sql_select().unwrap();
        let sql_str = sql_expr.sql(&Dialect::datafusion()).unwrap();
        assert_eq!(sql_str, "(\"a\" + 23) AS \"foo\"");
    }
}
