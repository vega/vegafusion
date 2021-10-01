use crate::error::VegaFusionError;
use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::lexer::Token;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;

/// ESTree-style AST Node for logical infix operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#logicaloperator
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub enum LogicalOperator {
    #[serde(rename = "||")]
    Or,

    #[serde(rename = "&&")]
    And,
}

impl LogicalOperator {
    pub fn infix_binding_power(&self) -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left
        match self {
            LogicalOperator::Or => (6.0, 6.5),
            LogicalOperator::And => (7.0, 7.5),
        }
    }
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Or => write!(f, "||"),
            Self::And => write!(f, "&&"),
        }
    }
}

impl TryFrom<&Token> for LogicalOperator {
    type Error = VegaFusionError;
    fn try_from(value: &Token) -> Result<Self, Self::Error> {
        Ok(match value {
            Token::LogicalOr => Self::Or,
            Token::LogicalAnd => Self::And,
            t => {
                return Err(VegaFusionError::parse_error(&format!(
                    "Token '{}' is not a valid logical operator",
                    t
                )))
            }
        })
    }
}

/// ESTree-style AST Node for logical infix expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#logicalexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct LogicalExpression {
    pub left: Box<Expression>,
    pub operator: LogicalOperator,
    pub right: Box<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for LogicalExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
    fn binding_power(&self) -> (f64, f64) {
        self.operator.infix_binding_power()
    }
}

impl fmt::Display for LogicalExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use binding power to determine whether lhs and/or rhs need parens
        let lhs_parens = self.left.binding_power().1 < self.binding_power().0;
        let rhs_parens = self.right.binding_power().0 < self.binding_power().1;

        // Write lhs
        if lhs_parens {
            write!(f, "({})", self.left)?;
        } else {
            write!(f, "{}", self.left)?;
        }

        write!(f, " {} ", self.operator)?;

        // Write rhs
        if rhs_parens {
            write!(f, "({})", self.right)
        } else {
            write!(f, "{}", self.right)
        }
    }
}
