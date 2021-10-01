use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::ast::identifier::Identifier;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Enum that serializes like Expression, but only includes the Identifier variant
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(tag = "type")]
pub enum Callee {
    Identifier(Identifier),
}

impl fmt::Display for Callee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Callee::Identifier(identifier) => {
                write!(f, "{}", identifier)
            }
        }
    }
}

/// ESTree-style AST Node for call expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#callexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct CallExpression {
    pub callee: Callee,
    pub arguments: Vec<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for CallExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for CallExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let internal_binding_power = 1.0;
        let mut arg_strings: Vec<String> = Vec::new();
        for arg in self.arguments.iter() {
            let arg_binding_power = arg.binding_power().0;
            let arg_string = if arg_binding_power > internal_binding_power {
                format!("{}", arg)
            } else {
                // e.g. the argument is a comma infix operation, so it must be wrapped in parens
                format!("({})", arg)
            };
            arg_strings.push(arg_string)
        }
        let args_string = arg_strings.join(", ");

        match &self.callee {
            Callee::Identifier(ident) => {
                write!(f, "{}({})", ident, args_string)
            }
        }
    }
}
