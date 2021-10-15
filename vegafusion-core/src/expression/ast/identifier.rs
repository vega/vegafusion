use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::Identifier;
use std::fmt::{Display, Formatter};

impl ExpressionTrait for Identifier {}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Identifier {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}
