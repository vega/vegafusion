use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use serde::{Deserialize, Serialize};
use std::fmt;

/// ESTree-style AST Node for conditional/ternary expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#conditionalexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct ConditionalExpression {
    pub test: Box<Expression>,
    pub consequent: Box<Expression>,
    pub alternate: Box<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ConditionalExpression {
    pub fn ternary_binding_power() -> (f64, f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        (4.8, 4.6, 4.4)
    }
}

impl ExpressionTrait for ConditionalExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }

    fn binding_power(&self) -> (f64, f64) {
        let (left_bp, _, right_bp) = Self::ternary_binding_power();
        (left_bp, right_bp)
    }
}

impl fmt::Display for ConditionalExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (left_bp, middle_bp, right_bp) = Self::ternary_binding_power();
        // Write test expression using binding power to determine whether parens are needed
        if self.test.binding_power().1 < left_bp {
            write!(f, "({})", self.test)?;
        } else {
            write!(f, "{}", self.test)?;
        }

        write!(f, " ? ")?;

        // Write consequent expression using binding power to determine whether parens are needed
        if self.consequent.binding_power().1 < middle_bp {
            write!(f, "({})", self.consequent)?;
        } else {
            write!(f, "{}", self.consequent)?;
        }

        write!(f, ": ")?;

        if self.alternate.binding_power().0 < right_bp {
            write!(f, "({})", self.alternate)
        } else {
            write!(f, "{}", self.alternate)
        }
    }
}
