/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{Expression, LogicalExpression, LogicalOperator};
use std::fmt::{Display, Formatter};

// Logical
impl Display for LogicalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalOperator::Or => write!(f, "||"),
            LogicalOperator::And => write!(f, "&&"),
        }
    }
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

impl LogicalExpression {
    pub fn new(lhs: Expression, op: &LogicalOperator, rhs: Expression) -> Self {
        Self {
            left: Some(Box::new(lhs)),
            operator: *op as i32,
            right: Some(Box::new(rhs)),
        }
    }

    pub fn to_operator(&self) -> LogicalOperator {
        LogicalOperator::from_i32(self.operator).unwrap()
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

impl ExpressionTrait for LogicalExpression {
    fn binding_power(&self) -> (f64, f64) {
        self.infix_binding_power()
    }
}

impl Display for LogicalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Use binding power to determine whether lhs and/or rhs need parens
        let lhs_bp = self.left().binding_power();
        let rhs_bp = self.right().binding_power();
        let self_bp = self.binding_power();
        let lhs_parens = lhs_bp.1 < self_bp.0;
        let rhs_parens = rhs_bp.0 < self_bp.1;

        // Write lhs
        if lhs_parens {
            write!(f, "({})", self.left())?;
        } else {
            write!(f, "{}", self.left())?;
        }

        write!(f, " {} ", self.to_operator())?;

        // Write rhs
        if rhs_parens {
            write!(f, "({})", self.right())
        } else {
            write!(f, "{}", self.right())
        }
    }
}
