use crate::error::{Result, VegaFusionError};
use crate::expression::ast::base::Expression;
use crate::expression::ast::conditional::ConditionalExpression;

/// `if(test, thenValue, elseValue)`
///
/// If test is truthy, returns thenValue. Otherwise, returns elseValue.
/// The if function is equivalent to the ternary operator a ? b : c.
///
/// See: https://vega.github.io/vega/docs/expressions/#if
pub fn if_fn(arguments: &[Expression]) -> Result<Expression> {
    if let [test, consequent, alternate] = arguments {
        Ok(Expression::from(ConditionalExpression {
            test: Box::new(test.clone()),
            consequent: Box::new(consequent.clone()),
            alternate: Box::new(alternate.clone()),
            span: None,
        }))
    } else {
        return Err(VegaFusionError::compilation(&format!(
            "The if function requires exactly 3 arguments. Received {}",
            arguments.len()
        )));
    }
}
