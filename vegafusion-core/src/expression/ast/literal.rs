use crate::expression::ast::expression::ExpressionTrait;
use crate::expression::ast::literal;
use crate::proto::gen::expression::literal::Value;
use crate::proto::gen::expression::Literal;
use std::fmt::{Display, Formatter};

impl Display for literal::Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Boolean(v) => {
                write!(f, "{}", v)
            }
            Value::Number(v) => {
                write!(f, "{}", v)
            }
            Value::String(v) => {
                write!(f, "\"{}\"", v)
            }
            Value::Null(v) => {
                write!(f, "null")
            }
        }
    }
}

impl ExpressionTrait for Literal {}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(value) = &self.value {
            write!(f, "{}", value)
        } else {
            write!(f, "None")
        }
    }
}
