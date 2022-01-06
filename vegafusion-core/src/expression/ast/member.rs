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
use crate::error::Result;
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{Expression, MemberExpression};
use std::fmt::{Display, Formatter};

impl MemberExpression {
    pub fn new_computed(object: Expression, property: Expression) -> Self {
        Self {
            object: Some(Box::new(object)),
            property: Some(Box::new(property)),
            computed: true,
        }
    }

    pub fn new_static(object: Expression, property: Expression) -> Result<Self> {
        // Make sure property is an identifier
        property.as_identifier()?;
        Ok(Self {
            object: Some(Box::new(object)),
            property: Some(Box::new(property)),
            computed: false,
        })
    }

    pub fn member_binding_power() -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left
        (20.0, 20.5)
    }

    pub fn property(&self) -> &Expression {
        self.property.as_ref().unwrap()
    }

    pub fn object(&self) -> &Expression {
        self.object.as_ref().unwrap()
    }
}

impl ExpressionTrait for MemberExpression {
    fn binding_power(&self) -> (f64, f64) {
        Self::member_binding_power()
    }
}

impl Display for MemberExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let object_rhs_bp = self.object.as_ref().unwrap().binding_power().1;

        // Write object, using binding power to determine whether to wrap it in parenthesis
        if object_rhs_bp < Self::member_binding_power().0 {
            write!(f, "({})", self.object.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.object.as_ref().unwrap())?;
        }

        // Write property
        if self.computed {
            write!(f, "[{}]", self.property.as_ref().unwrap())
        } else {
            write!(f, ".{}", self.property.as_ref().unwrap())
        }
    }
}
