/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
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
            Value::Null(_v) => {
                write!(f, "null")
            }
        }
    }
}

impl From<bool> for literal::Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<f64> for literal::Value {
    fn from(v: f64) -> Self {
        Value::Number(v)
    }
}

impl From<&str> for literal::Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<String> for literal::Value {
    fn from(v: String) -> Self {
        Value::String(v)
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

impl Literal {
    pub fn new<V: Into<literal::Value>>(v: V, raw: &str) -> Self {
        Self {
            raw: raw.to_string(),
            value: Some(v.into()),
        }
    }

    pub fn null() -> Self {
        Self {
            raw: "null".to_string(),
            value: Some(literal::Value::Null(false)),
        }
    }

    pub fn value(&self) -> &literal::Value {
        self.value.as_ref().unwrap()
    }
}
