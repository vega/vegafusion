
use datafusion_expr::Expr;
use sqlgen::ast::{OrderByExpr as SqlOrderByExpr};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use crate::sql::compile::expr::ToSqlExpr;


pub trait ToSqlOrderByExpr {
    fn to_sql_order(&self) -> Result<SqlOrderByExpr>;
}

impl ToSqlOrderByExpr for Expr {
    fn to_sql_order(&self) -> Result<SqlOrderByExpr> {
        match self {
            Expr::Sort {
                expr, asc, nulls_first
            } => {
                Ok(SqlOrderByExpr {
                    expr: expr.to_sql().with_context(|| format!("Expression cannot be used as order by expression: {:?}", expr))?,
                    asc: Some(*asc),
                    nulls_first: Some(*nulls_first)
                })
            }
            _ => Err(VegaFusionError::internal("Only Sort expressions may be converted to OrderByExpr AST nodes"))
        }
    }
}


#[cfg(test)]
mod tests {
    
    use sqlgen::dialect::DialectDisplay;
    use datafusion_expr::{col, Expr};
    
    use crate::sql::compile::order::ToSqlOrderByExpr;

    #[test]
    pub fn test_non_sort_expr() {
        let sort_expr = col("a");
        sort_expr.to_sql_order().unwrap_err();
    }

    #[test]
    pub fn test_sort_by_col() {
        let sort_expr = Expr::Sort {
            expr: Box::new(col("a")),
            asc: false,
            nulls_first: false
        };

        let sort_sql = sort_expr.to_sql_order().unwrap();
        let sql_str = sort_sql.sql(&Default::default()).unwrap();
        assert_eq!(sql_str, "a DESC NULLS LAST".to_string());
    }
}