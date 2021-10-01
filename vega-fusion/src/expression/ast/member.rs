use crate::expression::ast::base::{Expression, ExpressionTrait, Span};
use crate::expression::ast::identifier::Identifier;
use serde::{Deserialize, Serialize};
use std::fmt;

/// ESTree-style AST Node for member/element lookup in object or array (e.g. `foo[bar]`))
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#property
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct MemberExpression {
    pub object: Box<Expression>,
    pub property: Box<Expression>,
    pub computed: bool,

    #[serde(skip)]
    pub span: Option<Span>,
}

impl MemberExpression {
    pub fn member_binding_power() -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left
        (20.0, 20.5)
    }

    pub fn new_computed<O: Into<Expression>, P: Into<Expression>>(
        object: O,
        property: P,
        span: Option<Span>,
    ) -> Self {
        Self {
            object: Box::new(object.into()),
            property: Box::new(property.into()),
            computed: true,
            span,
        }
    }

    pub fn new_static<O: Into<Expression>, P: Into<Identifier>>(
        object: O,
        property: P,
        span: Option<Span>,
    ) -> Self {
        Self {
            object: Box::new(object.into()),
            property: Box::new(Expression::from(property.into())),
            computed: false,
            span,
        }
    }
}

impl ExpressionTrait for MemberExpression {
    fn span(&self) -> Option<Span> {
        self.span
    }

    fn binding_power(&self) -> (f64, f64) {
        Self::member_binding_power()
    }
}

impl fmt::Display for MemberExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object_rhs_bp = self.object.binding_power().1;

        // Write object, using binding power to determine whether to wrap it in parenthesis
        if object_rhs_bp < Self::member_binding_power().0 {
            write!(f, "({})", self.object)?;
        } else {
            write!(f, "{}", self.object)?;
        }

        // Write property
        if self.computed {
            write!(f, "[{}]", self.property)
        } else {
            write!(f, ".{}", self.property)
        }
    }
}
