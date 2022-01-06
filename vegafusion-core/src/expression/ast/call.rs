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
use crate::proto::gen::expression::{CallExpression, Expression};
use std::fmt::{Display, Formatter};

impl CallExpression {
    pub fn new(callee: &str, arguments: Vec<Expression>) -> Self {
        Self {
            callee: callee.to_string(),
            arguments,
        }
    }
}

impl ExpressionTrait for CallExpression {}

impl Display for CallExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let internal_binding_power = 1.0;
        let mut arg_strings: Vec<String> = Vec::new();
        for arg in self.arguments.iter() {
            let arg_binding_power = arg.binding_power().0;
            let arg_string = if arg_binding_power > internal_binding_power {
                format!("{}", arg)
            } else {
                // e.g. the argument is a comma infix operation, so it must be wrapped in parens
                format!("({})", arg)
            };
            arg_strings.push(arg_string)
        }
        let args_string = arg_strings.join(", ");

        write!(f, "{}({})", self.callee, args_string)
    }
}
