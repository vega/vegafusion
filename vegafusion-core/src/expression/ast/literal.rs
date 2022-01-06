/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
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
