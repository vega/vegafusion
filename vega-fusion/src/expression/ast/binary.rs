use crate::error::VegaFusionError;
use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::lexer::Token;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::{fmt, fmt::Formatter};

/// ESTree-style AST Node for binary infix operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#binaryoperator
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub enum BinaryOperator {
    #[serde(rename = "==")]
    Equals,

    #[serde(rename = "!=")]
    NotEquals,

    #[serde(rename = "===")]
    StrictEquals,

    #[serde(rename = "!==")]
    NotStrictEquals,

    #[serde(rename = "<")]
    LessThan,

    #[serde(rename = "<=")]
    LessThanEqual,

    #[serde(rename = ">")]
    GreaterThan,

    #[serde(rename = ">=")]
    GreaterThanEqual,

    #[serde(rename = "+")]
    Plus,

    #[serde(rename = "-")]
    Minus,

    #[serde(rename = "*")]
    Mult,

    #[serde(rename = "/")]
    Div,

    #[serde(rename = "%")]
    Mod,
}

impl BinaryOperator {
    pub fn infix_binding_power(&self) -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left

        match self {
            BinaryOperator::Plus | BinaryOperator::Minus => (14.0, 14.5),
            BinaryOperator::Mult | BinaryOperator::Div | BinaryOperator::Mod => (15.0, 15.5),
            BinaryOperator::GreaterThan
            | BinaryOperator::LessThan
            | BinaryOperator::GreaterThanEqual
            | BinaryOperator::LessThanEqual => (12.0, 12.5),
            BinaryOperator::Equals
            | BinaryOperator::StrictEquals
            | BinaryOperator::NotEquals
            | BinaryOperator::NotStrictEquals => (11.0, 11.5),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Mult => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Mod => write!(f, "%"),
            BinaryOperator::Equals => write!(f, "=="),
            BinaryOperator::StrictEquals => write!(f, "==="),
            BinaryOperator::NotEquals => write!(f, "!="),
            BinaryOperator::NotStrictEquals => write!(f, "!=="),
            BinaryOperator::GreaterThan => write!(f, ">"),
            BinaryOperator::LessThan => write!(f, "<"),
            BinaryOperator::GreaterThanEqual => write!(f, ">="),
            BinaryOperator::LessThanEqual => write!(f, "<="),
        }
    }
}

impl TryFrom<&Token> for BinaryOperator {
    type Error = VegaFusionError;

    fn try_from(value: &Token) -> Result<Self, Self::Error> {
        Ok(match value {
            Token::Plus => Self::Plus,
            Token::Minus => Self::Minus,
            Token::Asterisk => Self::Mult,
            Token::Slash => Self::Div,
            Token::Percent => Self::Mod,
            Token::DoubleEquals => Self::Equals,
            Token::TripleEquals => Self::StrictEquals,
            Token::ExclamationEquals => Self::NotEquals,
            Token::ExclamationDoubleEquals => Self::NotStrictEquals,
            Token::GreaterThan => Self::GreaterThan,
            Token::GreaterThanEquals => Self::GreaterThanEqual,
            Token::LessThan => Self::LessThan,
            Token::LessThanEquals => Self::LessThanEqual,
            t => {
                return Err(VegaFusionError::parse(&format!(
                    "Token '{}' is not a valid binary operator",
                    t
                )))
            }
        })
    }
}

/// ESTree-style AST Node for binary infix expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#binaryexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct BinaryExpression {
    pub left: Box<Expression>,
    pub operator: BinaryOperator,
    pub right: Box<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for BinaryExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
    fn binding_power(&self) -> (f64, f64) {
        self.operator.infix_binding_power()
    }
}

impl fmt::Display for BinaryExpression {
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
