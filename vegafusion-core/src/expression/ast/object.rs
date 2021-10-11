use std::fmt::{Display, Formatter};
use crate::proto::gen::expression::property::Key;
use crate::proto::gen::expression::{Property, ObjectExpression};
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::literal::Value;

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Literal(v) => write!(f, "{}", v.value.as_ref().unwrap()),
            Key::Identifier(v) => write!(f, "{}", v),
        }
    }
}

impl Key {
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

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key.as_ref().unwrap(), self.value.as_ref().unwrap())
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
