use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use sqlparser::ast::{Ident, SelectItem as SqlSelectItem};
use vegafusion_common::error::Result;

pub trait ToSqlSelectItem {
    fn to_sql_select(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlSelectItem>;
}

impl ToSqlSelectItem for Expr {
    fn to_sql_select(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlSelectItem> {
        Ok(match self {
            Expr::Alias(expr, alias) => SqlSelectItem::ExprWithAlias {
                expr: expr.to_sql(dialect, schema)?,
                alias: Ident {
                    value: alias.clone(),
                    quote_style: Some(dialect.quote_style),
                },
            },
            Expr::Wildcard => SqlSelectItem::Wildcard(Default::default()),
            expr => SqlSelectItem::UnnamedExpr(expr.to_sql(dialect, schema)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::compile::select::ToSqlSelectItem;
    use datafusion_expr::{col, lit, Expr};
    use std::ops::Add;
    use arrow::datatypes::Schema;
    use datafusion_common::DFSchema;
    use crate::dialect::Dialect;

    fn schema() -> DFSchema {
        DFSchema::try_from(Schema::new(Vec::new())).unwrap()
    }

    #[test]
    pub fn test_select_wildcard() {
        let expr = Expr::Wildcard;
        let sql_expr = expr.to_sql_select(&Dialect::datafusion(), &schema()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "*");
    }

    #[test]
    pub fn test_select_unnamed_expr() {
        let expr = col("a").add(lit(23));
        let sql_expr = expr.to_sql_select(&Dialect::datafusion(), &schema()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23)");
    }

    #[test]
    pub fn test_select_aliased_expr() {
        let expr = col("a").add(lit(23)).alias("foo");
        let sql_expr = expr.to_sql_select(&Dialect::datafusion(), &schema()).unwrap();
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(\"a\" + 23) AS \"foo\"");
    }
}
