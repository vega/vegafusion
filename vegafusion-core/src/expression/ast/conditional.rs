/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{ConditionalExpression, Expression};
use std::fmt::{Display, Formatter};

impl ConditionalExpression {
    pub fn new(test: Expression, consequent: Expression, alternate: Expression) -> Self {
        Self {
            test: Some(Box::new(test)),
            consequent: Some(Box::new(consequent)),
            alternate: Some(Box::new(alternate)),
        }
    }

    pub fn ternary_binding_power() -> (f64, f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        (4.8, 4.6, 4.4)
    }

    pub fn test(&self) -> &Expression {
        self.test.as_ref().unwrap()
    }

    pub fn alternate(&self) -> &Expression {
        self.alternate.as_ref().unwrap()
    }

    pub fn consequent(&self) -> &Expression {
        self.consequent.as_ref().unwrap()
    }
}

impl ExpressionTrait for ConditionalExpression {
    fn binding_power(&self) -> (f64, f64) {
        let (left_bp, _, right_bp) = Self::ternary_binding_power();
        (left_bp, right_bp)
    }
}

impl Display for ConditionalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (left_bp, middle_bp, right_bp) = Self::ternary_binding_power();
        // Write test expression using binding power to determine whether parens are needed
        let test_bp = self.test.as_ref().unwrap().binding_power();
        let consequent_bp = self.consequent.as_ref().unwrap().binding_power();
        let alternate_bp = self.alternate.as_ref().unwrap().binding_power();

        if test_bp.1 < left_bp {
            write!(f, "({})", self.test.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.test.as_ref().unwrap())?;
        }

        write!(f, " ? ")?;

        // Write consequent expression using binding power to determine whether parens are needed
        if consequent_bp.1 < middle_bp {
            write!(f, "({})", self.consequent.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.consequent.as_ref().unwrap())?;
        }

        write!(f, ": ")?;

        if alternate_bp.0 < right_bp {
            write!(f, "({})", self.alternate.as_ref().unwrap())
        } else {
            write!(f, "{}", self.alternate.as_ref().unwrap())
        }
    }
}
