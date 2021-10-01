use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use serde::{Deserialize, Serialize};
use std::fmt;

/// ESTree-style AST Node for array literal (e.g. [23, "Hello"])
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#arrayexpression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct ArrayExpression {
    pub elements: Vec<Expression>,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl ExpressionTrait for ArrayExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for ArrayExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
