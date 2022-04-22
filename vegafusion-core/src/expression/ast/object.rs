/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::literal::Value;
use crate::proto::gen::expression::property::Key;
use crate::proto::gen::expression::{Expression, Identifier, Literal, ObjectExpression, Property};
use std::fmt::{Display, Formatter};

use crate::error::{Result, VegaFusionError};

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Literal(v) => write!(f, "{}", v.value.as_ref().unwrap()),
            Key::Identifier(v) => write!(f, "{}", v),
        }
    }
}

impl Key {
    pub fn new_literal(key: Literal) -> Self {
        Key::Literal(key)
    }

    pub fn new_identifier(key: Identifier) -> Self {
        Self::Identifier(key)
    }

    // Get string for property for use as object key. Strings should not be quoted
    pub fn to_object_key_string(&self) -> String {
        match self {
            Key::Literal(v) => match v.value.as_ref().unwrap() {
                Value::String(s) => s.clone(),
                _ => v.to_string(),
            },
            Key::Identifier(v) => v.name.clone(),
        }
    }
}

impl Property {
    pub fn try_new(key: Expression, value: Expression) -> Result<Self> {
        if let Ok(identifier) = key.as_identifier() {
            Ok(Self::new_identifier(identifier.clone(), value))
        } else if let Ok(literal) = key.as_literal() {
            Ok(Self::new_literal(literal.clone(), value))
        } else {
            Err(VegaFusionError::internal(
                "Object key must be an identifier or a literal value",
            ))
        }
    }

    pub fn new_literal(key: Literal, value: Expression) -> Self {
        Self {
            value: Some(value),
            key: Some(Key::new_literal(key)),
            kind: "init".to_string(),
        }
    }

    pub fn new_identifier(key: Identifier, value: Expression) -> Self {
        Self {
            value: Some(value),
            key: Some(Key::new_identifier(key)),
            kind: "init".to_string(),
        }
    }

    pub fn key(&self) -> &Key {
        self.key.as_ref().unwrap()
    }

    pub fn value(&self) -> &Expression {
        self.value.as_ref().unwrap()
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key(), self.value())
    }
}

impl ObjectExpression {
    pub fn new(properties: Vec<Property>) -> Self {
        Self { properties }
    }
}

impl ExpressionTrait for ObjectExpression {}

impl Display for ObjectExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let property_strings: Vec<String> = self.properties.iter().map(|p| p.to_string()).collect();
        let property_csv = property_strings.join(", ");
        write!(f, "{{{}}}", property_csv)
    }
}
