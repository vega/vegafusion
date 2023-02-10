use crate::escape::unescape_field;
use datafusion_common::Column;
use datafusion_expr::Expr;

pub fn flat_col(col_name: &str) -> Expr {
    Expr::Column(Column::from_name(col_name))
}

pub fn unescaped_col(col_name: &str) -> Expr {
    flat_col(&unescape_field(col_name))
}
