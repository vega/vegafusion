use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use datafusion_common::DFSchema;
use datafusion_expr::SortExpr;
use sqlparser::ast::OrderByExpr as SqlOrderByExpr;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

pub trait ToSqlOrderByExpr {
    fn to_sql_order(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlOrderByExpr>;
}

impl ToSqlOrderByExpr for SortExpr {
    fn to_sql_order(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlOrderByExpr> {
        let nulls_first = if dialect.supports_null_ordering {
            // Be explicit about null ordering
            Some(self.nulls_first)
        } else {
            // If null ordering is not supported, then don't specify it as long the as default
            // behavior matches what's specified.
            if (self.asc && self.nulls_first) || (!self.asc && !self.nulls_first) {
                None
            } else {
                return Err(VegaFusionError::sql_not_supported(
                    "Dialect does not support NULL ordering",
                ));
            }
        };

        Ok(SqlOrderByExpr {
            expr: self.expr.to_sql(dialect, schema).with_context(|| {
                format!("Expression cannot be used as order by expression: {expr:?}", expr=self.expr)
            })?,
            asc: Some(self.asc),
            nulls_first,
            with_fill: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::compile::order::ToSqlOrderByExpr;
    use datafusion_common::DFSchema;
    use datafusion_expr::expr;
    use vegafusion_common::column::flat_col;

    fn schema() -> DFSchema {
        DFSchema::empty()
    }

    #[test]
    pub fn test_sort_by_col() {
        let sort_expr = expr::Sort {
            expr: flat_col("a"),
            asc: false,
            nulls_first: false,
        };

        let sort_sql = sort_expr
            .to_sql_order(&Default::default(), &schema())
            .unwrap();
        let sql_str = sort_sql.to_string();
        assert_eq!(sql_str, r#""a" DESC NULLS LAST"#.to_string());
    }
}
