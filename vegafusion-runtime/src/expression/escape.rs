use datafusion::common::Column;
use datafusion_expr::Expr;
use vegafusion_core::expression::escape::unescape_field;

pub fn flat_col(col_name: &str) -> Expr {
    Expr::Column(Column::from_name(col_name))
}

pub fn unescaped_col(col_name: &str) -> Expr {
    flat_col(&unescape_field(col_name))
}
