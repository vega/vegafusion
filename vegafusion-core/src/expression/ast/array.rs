/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::ast::expression::ExpressionTrait;
use crate::proto::gen::expression::{ArrayExpression, Expression};
use std::fmt::{Display, Formatter};

impl ArrayExpression {
    pub fn new(elements: Vec<Expression>) -> Self {
        Self { elements }
    }
}

impl ExpressionTrait for ArrayExpression {}

impl Display for ArrayExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let internal_binding_power = 1.0;
        let mut element_strings: Vec<String> = Vec::new();
        for element in self.elements.iter() {
            let arg_binding_power = element.binding_power().0;
            let element_string = if arg_binding_power > internal_binding_power {
                format!("{}", element)
            } else {
                // e.g. the argument is a comma infix operation, so it must be wrapped in parens
                format!("({})", element)
            };
            element_strings.push(element_string)
        }

        let elements_string = element_strings.join(", ");
        write!(f, "[{}]", elements_string)
    }
}
