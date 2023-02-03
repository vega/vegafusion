use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::dialect::Dialect;
use datafusion_expr::Expr;
use sqlparser::ast::{Ident, SelectItem as SqlSelectItem};
use vegafusion_core::error::Result;

pub trait ToSqlSelectItem {
    fn to_sql_select(&self, dialect: &Dialect) -> Result<SqlSelectItem>;
}

impl ToSqlSelectItem for Expr {
    fn to_sql_select(&self, dialect: &Dialect) -> Result<SqlSelectItem> {
        Ok(match self {
            Expr::Alias(expr, alias) => SqlSelectItem::ExprWithAlias {
                expr: expr.to_sql(dialect)?,
                alias: Ident {
                    value: alias.clone(),
                    quote_style: Some('"'),
                },
            },
            Expr::Wildcard => SqlSelectItem::Wildcard(Default::default()),
            expr => SqlSelectItem::UnnamedExpr(expr.to_sql(dialect)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use crate::expression::escape::flat_col;
    use crate::sql::compile::select::ToSqlSelectItem;
    use datafusion_expr::{lit, Expr};

    #[test]
    pub fn test_select_wildcard() {
        let expr = Expr::Wildcard;
        let sql_expr = expr.to_sql_select(&Default::default()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "*");
    }

    #[test]
    pub fn test_select_unnamed_expr() {
        let expr = flat_col("a").add(lit(23));
        let sql_expr = expr.to_sql_select(&Default::default()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23)");
    }

    #[test]
    pub fn test_select_aliased_expr() {
        let expr = flat_col("a").add(lit(23)).alias("foo");
        let sql_expr = expr.to_sql_select(&Default::default()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23) AS \"foo\"");
    }
}
