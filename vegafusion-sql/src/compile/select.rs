use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use datafusion_common::DFSchema;
use datafusion_expr::{expr, Expr};
use sqlparser::ast::{Ident, ObjectName, SelectItem as SqlSelectItem};
use vegafusion_common::error::Result;

pub trait ToSqlSelectItem {
    fn to_sql_select(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlSelectItem>;
}

impl ToSqlSelectItem for Expr {
    fn to_sql_select(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlSelectItem> {
        Ok(match self {
            Expr::Alias(expr::Alias {
                expr, name: alias, ..
            }) => SqlSelectItem::ExprWithAlias {
                expr: expr.to_sql(dialect, schema)?,
                alias: Ident {
                    value: alias.clone(),
                    quote_style: Some(dialect.quote_style),
                },
            },
            Expr::Wildcard {
                qualifier: None,
                options: _,
            } => SqlSelectItem::Wildcard(Default::default()),
            Expr::Wildcard {
                qualifier: Some(qualifier),
                options: _,
            } => SqlSelectItem::QualifiedWildcard(
                ObjectName(vec![Ident {
                    value: qualifier.to_string(),
                    quote_style: Some(dialect.quote_style),
                }]),
                Default::default(),
            ),
            expr => SqlSelectItem::UnnamedExpr(expr.to_sql(dialect, schema)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::compile::select::ToSqlSelectItem;
    use crate::dialect::Dialect;
    use datafusion_common::DFSchema;
    use datafusion_expr::expr::WildcardOptions;
    use datafusion_expr::{lit, Expr};
    use std::ops::Add;
    use vegafusion_common::column::flat_col;

    fn schema() -> DFSchema {
        DFSchema::empty()
    }

    #[test]
    pub fn test_select_wildcard() {
        let expr = Expr::Wildcard {
            qualifier: None,
            options: WildcardOptions::default(),
        };
        let sql_expr = expr
            .to_sql_select(&Dialect::datafusion(), &schema())
            .unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "*");
    }

    #[test]
    pub fn test_select_unnamed_expr() {
        let expr = flat_col("a").add(lit(23));
        let sql_expr = expr
            .to_sql_select(&Dialect::datafusion(), &schema())
            .unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23)");
    }

    #[test]
    pub fn test_select_aliased_expr() {
        let expr = flat_col("a").add(lit(23)).alias("foo");
        let sql_expr = expr
            .to_sql_select(&Dialect::datafusion(), &schema())
            .unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23) AS \"foo\"");
    }
}
