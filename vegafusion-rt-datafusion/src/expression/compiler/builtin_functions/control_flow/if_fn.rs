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
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::expression::expression::Expr;
use vegafusion_core::proto::gen::expression::{ConditionalExpression, Expression};

/// `if(test, thenValue, elseValue)`
///
/// If test is truthy, returns thenValue. Otherwise, returns elseValue.
/// The if function is equivalent to the ternary operator a ? b : c.
///
/// See: https://vega.github.io/vega/docs/expressions/#if
pub fn if_fn(arguments: &[Expression]) -> Result<Expression> {
    if let [test, consequent, alternate] = arguments {
        let expr = Expr::Conditional(Box::new(ConditionalExpression {
            test: Some(Box::new(test.clone())),
            consequent: Some(Box::new(consequent.clone())),
            alternate: Some(Box::new(alternate.clone())),
        }));
        Ok(Expression {
            expr: Some(expr),
            span: None,
        })
    } else {
        return Err(VegaFusionError::compilation(&format!(
            "The if function requires exactly 3 arguments. Received {}",
            arguments.len()
        )));
    }
}
