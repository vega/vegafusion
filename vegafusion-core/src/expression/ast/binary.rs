/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{BinaryExpression, BinaryOperator, Expression};
use std::fmt::{Display, Formatter};

// Binary
impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl BinaryExpression {
    pub fn new(lhs: Expression, op: &BinaryOperator, rhs: Expression) -> Self {
        Self {
            left: Some(Box::new(lhs)),
            operator: *op as i32,
            right: Some(Box::new(rhs)),
        }
    }

    pub fn to_operator(&self) -> BinaryOperator {
        BinaryOperator::from_i32(self.operator).unwrap()
    }

    pub fn infix_binding_power(&self) -> (f64, f64) {
        self.to_operator().infix_binding_power()
    }

    pub fn left(&self) -> &Expression {
        self.left.as_ref().unwrap()
    }

    pub fn right(&self) -> &Expression {
        self.right.as_ref().unwrap()
    }
}

impl ExpressionTrait for BinaryExpression {
    fn binding_power(&self) -> (f64, f64) {
        self.infix_binding_power()
    }
}

impl Display for BinaryExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Use binding power to determine whether lhs and/or rhs need parens
        let lhs_bp = self.left.as_ref().unwrap().binding_power();
        let rhs_bp = self.right.as_ref().unwrap().binding_power();
        let self_bp = self.binding_power();
        let lhs_parens = lhs_bp.1 < self_bp.0;
        let rhs_parens = rhs_bp.0 < self_bp.1;

        // Write lhs
        if lhs_parens {
            write!(f, "({})", self.left.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.left.as_ref().unwrap())?;
        }

        write!(f, " {} ", self.to_operator())?;

        // Write rhs
        if rhs_parens {
            write!(f, "({})", self.right.as_ref().unwrap())
        } else {
            write!(f, "{}", self.right.as_ref().unwrap())
        }
    }
}
