/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{Expression, UnaryExpression, UnaryOperator};
use std::fmt::{Display, Formatter};
use std::ops::Deref;

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Pos => write!(f, "+"),
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "!"),
        }
    }
}

impl UnaryOperator {
    pub fn unary_binding_power(&self) -> f64 {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        match self {
            UnaryOperator::Neg | UnaryOperator::Pos | UnaryOperator::Not => 17.0,
        }
    }
}

impl UnaryExpression {
    pub fn unary_binding_power(&self) -> f64 {
        self.to_operator().unary_binding_power()
    }

    pub fn to_operator(&self) -> UnaryOperator {
        UnaryOperator::from_i32(self.operator).unwrap()
    }

    pub fn new(op: &UnaryOperator, arg: Expression) -> Self {
        Self {
            operator: *op as i32,
            prefix: true,
            argument: Some(Box::new(arg)),
        }
    }

    pub fn argument(&self) -> &Expression {
        self.argument.as_ref().unwrap()
    }
}

impl ExpressionTrait for UnaryExpression {
    fn binding_power(&self) -> (f64, f64) {
        let power = self.unary_binding_power();
        (power, power)
    }
}

impl Display for UnaryExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (_, arg_right_bp) = self.argument.as_ref().unwrap().deref().binding_power();
        let (self_left_bp, _) = self.binding_power();
        let op = self.to_operator();
        if self_left_bp > arg_right_bp {
            write!(f, "{}({})", op, self.argument.as_ref().unwrap())
        } else {
            write!(f, "{}{}", op, self.argument.as_ref().unwrap())
        }
    }
}
