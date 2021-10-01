use serde::{Deserialize, Serialize};
use std::fmt;

use crate::expression::ast::base::{ExpressionTrait, Span};

/// ESTree-style AST Node for identifiers
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#identifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct Identifier {
    pub name: String,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for Identifier {
    fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
