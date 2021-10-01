use crate::expression::ast::base::{ExpressionTrait, Span};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::{fmt, fmt::Formatter};

/// AST Node for liter scalar values
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    String(String),
    Boolean(bool),
    Number(f64),
    Null,
}

impl Hash for LiteralValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            LiteralValue::String(v) => ("String", v).hash(state),
            LiteralValue::Boolean(v) => ("Boolean", v).hash(state),
            LiteralValue::Number(v) => ("Number", OrderedFloat(*v)).hash(state),
            LiteralValue::Null => ("Null",).hash(state),
        }
    }
}

impl PartialEq for LiteralValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LiteralValue::String(v1), LiteralValue::String(v2)) => v1 == v2,
            (LiteralValue::Boolean(v1), LiteralValue::Boolean(v2)) => v1 == v2,
            (LiteralValue::Number(v1), LiteralValue::Number(v2)) => {
                OrderedFloat(*v1) == OrderedFloat(*v2)
            }
            (LiteralValue::Null, LiteralValue::Null) => true,
            _ => false,
        }
    }
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Boolean(v) => {
                write!(f, "{}", v)
            }
            LiteralValue::Number(v) => {
                write!(f, "{}", v)
            }
            LiteralValue::String(v) => {
                write!(f, "\"{}\"", v)
            }
            LiteralValue::Null => {
                write!(f, "null")
            }
        }
    }
}

impl From<bool> for LiteralValue {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<f64> for LiteralValue {
    fn from(v: f64) -> Self {
        Self::Number(v)
    }
}

impl From<&str> for LiteralValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

/// ESTree-style AST Node for literal value
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct Literal {
    pub value: LiteralValue,
    pub raw: String,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for Literal {
    fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}
