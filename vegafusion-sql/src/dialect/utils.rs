use sqlparser::ast::{Expr as SqlExpr, Value as SqlValue};

pub fn make_utc_expr() -> SqlExpr {
    SqlExpr::Value(SqlValue::SingleQuotedString("UTC".to_string()))
}
