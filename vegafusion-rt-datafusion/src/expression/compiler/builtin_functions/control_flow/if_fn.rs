/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
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
        Err(VegaFusionError::compilation(&format!(
            "The if function requires exactly 3 arguments. Received {}",
            arguments.len()
        )))
    }
}
