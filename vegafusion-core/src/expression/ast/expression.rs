use crate::error::{Result, VegaFusionError};
use crate::expression::visitors::{
    ClearSpansVisitor, ExpressionVisitor, GetVariablesVisitor, MutExpressionVisitor,
};
use crate::proto::gen::expression::expression::Expr;
use crate::proto::gen::expression::{
    ArrayExpression, BinaryExpression, CallExpression, ConditionalExpression, Expression,
    Identifier, Literal, LogicalExpression, MemberExpression, ObjectExpression, Span,
    UnaryExpression,
};
use itertools::sorted;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use crate::proto::gen::tasks::Variable;

/// Trait that all AST node types implement
pub trait ExpressionTrait: Display {
    /// Get the left and right binding power of this expression.
    /// When there is ambiguity in associativity, the expression with the lower binding power
    /// must be parenthesized
    fn binding_power(&self) -> (f64, f64) {
        (1000.0, 1000.0)
    }
}

impl Deref for Expression {
    type Target = dyn ExpressionTrait;

    fn deref(&self) -> &Self::Target {
        match self.expr.as_ref().unwrap() {
            Expr::Identifier(expr) => expr,
            Expr::Literal(expr) => expr,
            Expr::Binary(expr) => expr.as_ref(),
            Expr::Logical(expr) => expr.as_ref(),
            Expr::Unary(expr) => expr.as_ref(),
            Expr::Conditional(expr) => expr.as_ref(),
            Expr::Call(expr) => expr,
            Expr::Array(expr) => expr,
            Expr::Object(expr) => expr,
            Expr::Member(expr) => expr.as_ref(),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let expr = self.deref();
        write!(f, "{}", expr)
    }
}

impl Expression {
    pub fn new(expr: Expr, span: Option<Span>) -> Self {
        Self {
            expr: Some(expr),
            span,
        }
    }

    pub fn clear_spans(&mut self) {
        let mut visitor = ClearSpansVisitor::new();
        self.walk_mut(&mut visitor);
    }

    pub fn get_variables(&self) -> Vec<Variable> {
        let mut visitor = GetVariablesVisitor::new();
        self.walk(&mut visitor);

        sorted(visitor.variables).collect()
    }

    /// Walk visitor through the expression tree in a DFS traversal
    pub fn walk(&self, visitor: &mut dyn ExpressionVisitor) {
        match self.expr.as_ref().unwrap() {
            Expr::Binary(node) => {
                node.left().walk(visitor);
                node.right().walk(visitor);
                visitor.visit_binary(node);
            }
            Expr::Logical(node) => {
                node.left().walk(visitor);
                node.right().walk(visitor);
                visitor.visit_logical(node);
            }
            Expr::Unary(node) => {
                node.argument().walk(visitor);
                visitor.visit_unary(node);
            }
            Expr::Conditional(node) => {
                node.test().walk(visitor);
                node.consequent().walk(visitor);
                node.alternate().walk(visitor);
                visitor.visit_conditional(node);
            }
            Expr::Literal(node) => {
                visitor.visit_literal(node);
            }
            Expr::Identifier(node) => {
                visitor.visit_identifier(node);
            }
            Expr::Call(node) => {
                let callee_id = Identifier {
                    name: node.callee.clone(),
                };
                visitor.visit_called_identifier(&callee_id, &node.arguments);
                for arg in &node.arguments {
                    arg.walk(visitor);
                }
                visitor.visit_call(node);
            }
            Expr::Array(node) => {
                for el in &node.elements {
                    el.walk(visitor);
                }
                visitor.visit_array(node);
            }
            Expr::Object(node) => {
                for prop in &node.properties {
                    visitor.visit_object_key(prop.key.as_ref().unwrap());
                    prop.value.as_ref().unwrap().walk(visitor);
                }
                visitor.visit_object(node);
            }
            Expr::Member(node) => {
                node.object.as_ref().unwrap().walk(visitor);
                let prop_expr = node.property.as_ref().unwrap().expr.as_ref().unwrap();
                if let Expr::Identifier(identifier) = prop_expr {
                    visitor.visit_static_member_identifier(identifier);
                }
                node.property.as_ref().unwrap().walk(visitor);
                visitor.visit_member(node);
            }
        }
        visitor.visit_expression(self);
    }

    pub fn walk_mut(&mut self, visitor: &mut dyn MutExpressionVisitor) {
        match self.expr.as_mut().unwrap() {
            Expr::Binary(node) => {
                node.left.as_mut().unwrap().walk_mut(visitor);
                node.right.as_mut().unwrap().walk_mut(visitor);
                visitor.visit_binary(node);
            }
            Expr::Logical(node) => {
                node.left.as_mut().unwrap().walk_mut(visitor);
                node.right.as_mut().unwrap().walk_mut(visitor);
                visitor.visit_logical(node);
            }
            Expr::Unary(node) => {
                node.argument.as_mut().unwrap().walk_mut(visitor);
                visitor.visit_unary(node);
            }
            Expr::Conditional(node) => {
                node.test.as_mut().unwrap().walk_mut(visitor);
                node.consequent.as_mut().unwrap().walk_mut(visitor);
                node.alternate.as_mut().unwrap().walk_mut(visitor);
                visitor.visit_conditional(node);
            }
            Expr::Literal(node) => {
                visitor.visit_literal(node);
            }
            Expr::Identifier(node) => {
                visitor.visit_identifier(node);
            }
            Expr::Call(node) => {
                let mut callee_id = Identifier {
                    name: node.callee.clone(),
                };
                visitor.visit_called_identifier(&mut callee_id, &mut node.arguments);
                for arg in &mut node.arguments {
                    arg.walk_mut(visitor);
                }
                visitor.visit_call(node);
            }
            Expr::Array(node) => {
                for el in &mut node.elements {
                    el.walk_mut(visitor);
                }
                visitor.visit_array(node);
            }
            Expr::Object(node) => {
                for prop in &mut node.properties {
                    visitor.visit_object_key(prop.key.as_mut().unwrap());
                    prop.value.as_mut().unwrap().walk_mut(visitor);
                }
                visitor.visit_object(node);
            }
            Expr::Member(node) => {
                node.object.as_mut().unwrap().walk_mut(visitor);
                let prop_expr = node.property.as_mut().unwrap().expr.as_mut().unwrap();
                if let Expr::Identifier(identifier) = prop_expr {
                    visitor.visit_static_member_identifier(identifier);
                }
                node.property.as_mut().unwrap().walk_mut(visitor);
                visitor.visit_member(node);
            }
        }
        visitor.visit_expression(self);
    }

    pub fn as_identifier(&self) -> Result<&Identifier> {
        match &self.expr {
            Some(Expr::Identifier(identifier)) => Ok(identifier),
            _ => Err(VegaFusionError::internal("Expression is not an identifier")),
        }
    }

    pub fn as_literal(&self) -> Result<&Literal> {
        match &self.expr {
            Some(Expr::Literal(value)) => Ok(value),
            _ => Err(VegaFusionError::internal("Expression is not a Literal")),
        }
    }

    pub fn expr(&self) -> &Expr {
        self.expr.as_ref().unwrap()
    }
}

// Expr conversions
impl From<Literal> for Expr {
    fn from(v: Literal) -> Self {
        Self::Literal(v)
    }
}

impl From<Identifier> for Expr {
    fn from(v: Identifier) -> Self {
        Self::Identifier(v)
    }
}

impl From<UnaryExpression> for Expr {
    fn from(v: UnaryExpression) -> Self {
        Self::Unary(Box::new(v))
    }
}

impl From<BinaryExpression> for Expr {
    fn from(v: BinaryExpression) -> Self {
        Self::Binary(Box::new(v))
    }
}

impl From<LogicalExpression> for Expr {
    fn from(v: LogicalExpression) -> Self {
        Self::Logical(Box::new(v))
    }
}

impl From<CallExpression> for Expr {
    fn from(v: CallExpression) -> Self {
        Self::Call(v)
    }
}

impl From<MemberExpression> for Expr {
    fn from(v: MemberExpression) -> Self {
        Self::Member(Box::new(v))
    }
}

impl From<ConditionalExpression> for Expr {
    fn from(v: ConditionalExpression) -> Self {
        Self::Conditional(Box::new(v))
    }
}

impl From<ArrayExpression> for Expr {
    fn from(v: ArrayExpression) -> Self {
        Self::Array(v)
    }
}

impl From<ObjectExpression> for Expr {
    fn from(v: ObjectExpression) -> Self {
        Self::Object(v)
    }
}
