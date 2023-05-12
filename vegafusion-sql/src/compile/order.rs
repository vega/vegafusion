use crate::compile::expr::ToSqlExpr;
use crate::dialect::Dialect;
use datafusion_common::DFSchema;
use datafusion_expr::{expr::Sort, Expr};
use sqlparser::ast::OrderByExpr as SqlOrderByExpr;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

pub trait ToSqlOrderByExpr {
    fn to_sql_order(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlOrderByExpr>;
}

impl ToSqlOrderByExpr for Expr {
    fn to_sql_order(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlOrderByExpr> {
        match self {
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => {
                let nulls_first = if dialect.supports_null_ordering {
                    // Be explicit about null ordering
                    Some(*nulls_first)
                } else {
                    // If null ordering is not supported, then don't specify it as long the as default
                    // behavior matches what's specified.
                    if (*asc && *nulls_first) || (!*asc && !*nulls_first) {
                        None
                    } else {
                        return Err(VegaFusionError::sql_not_supported(
                            "Dialect does not support NULL ordering",
                        ));
                    }
                };

                Ok(SqlOrderByExpr {
                    expr: expr.to_sql(dialect, schema).with_context(|| {
                        format!("Expression cannot be used as order by expression: {expr:?}")
                    })?,
                    asc: Some(*asc),
                    nulls_first,
                })
            }
            _ => Err(VegaFusionError::internal(
                "Only Sort expressions may be converted to OrderByExpr AST nodes",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::compile::order::ToSqlOrderByExpr;
    use datafusion_common::DFSchema;
    use datafusion_expr::{expr, Expr};
    use vegafusion_common::column::flat_col;

    fn schema() -> DFSchema {
        DFSchema::empty()
    }

    #[test]
    pub fn test_non_sort_expr() {
        let sort_expr = flat_col("a");
        sort_expr
            .to_sql_order(&Default::default(), &schema())
            .unwrap_err();
    }

    #[test]
    pub fn test_sort_by_col() {
        let sort_expr = Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("a")),
            asc: false,
            nulls_first: false,
        });

        let sort_sql = sort_expr
            .to_sql_order(&Default::default(), &schema())
            .unwrap();
        let sql_str = sort_sql.to_string();
        assert_eq!(sql_str, r#""a" DESC NULLS LAST"#.to_string());
    }
}
