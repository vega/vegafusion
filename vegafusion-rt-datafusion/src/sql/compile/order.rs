use crate::sql::compile::expr::ToSqlExpr;
use datafusion_expr::{expr::Sort, Expr};
use sqlgen::ast::OrderByExpr as SqlOrderByExpr;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

pub trait ToSqlOrderByExpr {
    fn to_sql_order(&self) -> Result<SqlOrderByExpr>;
}

impl ToSqlOrderByExpr for Expr {
    fn to_sql_order(&self) -> Result<SqlOrderByExpr> {
        match self {
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Ok(SqlOrderByExpr {
                expr: expr.to_sql().with_context(|| {
                    format!(
                        "Expression cannot be used as order by expression: {:?}",
                        expr
                    )
                })?,
                asc: Some(*asc),
                nulls_first: Some(*nulls_first),
            }),
            _ => Err(VegaFusionError::internal(
                "Only Sort expressions may be converted to OrderByExpr AST nodes",
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::expression::escape::flat_col;
    use datafusion_expr::{expr, Expr};
    use sqlgen::dialect::DialectDisplay;

    use crate::sql::compile::order::ToSqlOrderByExpr;

    #[test]
    pub fn test_non_sort_expr() {
        let sort_expr = flat_col("a");
        sort_expr.to_sql_order().unwrap_err();
    }

    #[test]
    pub fn test_sort_by_col() {
        let sort_expr = Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("a")),
            asc: false,
            nulls_first: false,
        });

        let sort_sql = sort_expr.to_sql_order().unwrap();
        let sql_str = sort_sql.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "a DESC NULLS LAST".to_string());
    }
}
