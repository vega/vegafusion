use crate::error::VegaFusionError;
use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::lexer::Token;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;

/// ESTree-style AST Node for unary operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#unaryoperator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryOperator {
    #[serde(rename = "+")]
    Pos,

    #[serde(rename = "-")]
    Neg,

    #[serde(rename = "!")]
    Not,
}

impl UnaryOperator {
    pub fn unary_binding_power(&self) -> f64 {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        match self {
            UnaryOperator::Neg | UnaryOperator::Pos | UnaryOperator::Not => 17.0,
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Pos => write!(f, "+"),
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "!"),
        }
    }
}

impl TryFrom<&Token> for UnaryOperator {
    type Error = VegaFusionError;
    fn try_from(value: &Token) -> Result<Self, Self::Error> {
        Ok(match value {
            Token::Plus => Self::Pos,
            Token::Minus => Self::Neg,
            Token::Exclamation => Self::Not,
            t => {
                return Err(VegaFusionError::parse(&format!(
                    "Token '{}' is not a valid prefix operator",
                    t
                )))
            }
        })
    }
}

/// ESTree-style AST Node for unary expressions
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#updateexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct UnaryExpression {
    pub operator: UnaryOperator,
    pub prefix: bool,
    pub argument: Box<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for UnaryExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
    fn binding_power(&self) -> (f64, f64) {
        let power = self.operator.unary_binding_power();
        (power, power)
    }
}

impl fmt::Display for UnaryExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use binding power to determine whether target expression should be wrapped in parens
        if self.binding_power().1 > self.argument.binding_power().0 {
            write!(f, "{}({})", self.operator, self.argument)
        } else {
            write!(f, "{}{}", self.operator, self.argument)
        }
    }
}
