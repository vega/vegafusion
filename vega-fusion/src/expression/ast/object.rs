use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::ast::identifier::Identifier;
use crate::expression::ast::literal::{Literal, LiteralValue};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(tag = "type")]
pub enum PropertyKey {
    Literal(Literal),
    Identifier(Identifier),
}

impl PropertyKey {
    // Get string for property for use as object key. Strings should not be quoted
    pub fn to_object_key_string(&self) -> String {
        match self {
            PropertyKey::Literal(v) => match &v.value {
                LiteralValue::String(s) => s.clone(),
                _ => v.to_string(),
            },
            PropertyKey::Identifier(v) => v.name.clone(),
        }
    }
}

impl fmt::Display for PropertyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PropertyKey::Literal(v) => v.to_string(),
            PropertyKey::Identifier(v) => v.to_string(),
        };
        write!(f, "{}", s)
    }
}

/// ESTree-style AST Node for object property (e.g. `a: 23`)
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#property
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct Property {
    pub key: PropertyKey,
    pub value: Expression,
    pub kind: String,
}

impl Property {
    pub fn new_identifier<I: Into<Identifier>, E: Into<Expression>>(key: I, value: E) -> Self {
        Self {
            key: PropertyKey::Identifier(key.into()),
            value: value.into(),
            kind: "init".to_string(),
        }
    }

    pub fn new_literal<L: Into<Literal>, E: Into<Expression>>(key: L, value: E) -> Self {
        Self {
            key: PropertyKey::Literal(key.into()),
            value: value.into(),
            kind: "init".to_string(),
        }
    }
}

impl fmt::Display for Property {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.value)
    }
}

/// ESTree-style AST Node for object literal (e.g. `{a: 23, "Hello": false}`)
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#objectexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct ObjectExpression {
    pub properties: Vec<Property>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for ObjectExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for ObjectExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property_strings: Vec<String> = self.properties.iter().map(|p| p.to_string()).collect();
        let property_csv = property_strings.join(", ");
        write!(f, "{{{}}}", property_csv)
    }
}
