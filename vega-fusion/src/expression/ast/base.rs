use crate::expression::ast::array::ArrayExpression;
use crate::expression::ast::binary::BinaryExpression;
use crate::expression::ast::call::{CallExpression, Callee};
use crate::expression::ast::conditional::ConditionalExpression;
use crate::expression::ast::identifier::Identifier;
use crate::expression::ast::literal::Literal;
use crate::expression::ast::logical::LogicalExpression;
use crate::expression::ast::member::MemberExpression;
use crate::expression::ast::object::ObjectExpression;
use crate::expression::ast::unary::UnaryExpression;
use crate::expression::visitors::{ClearSpanVisitor, ExpressionVisitor, MutExpressionVisitor};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{fmt, fmt::Formatter};

/// A Span holds the start and end indices of an AstNode in the expression source code
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct Span(pub usize, pub usize);

/// Expression enum that serializes to ESTree compatible JSON object
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(tag = "type")]
pub enum Expression {
    Identifier(Identifier),
    Literal(Literal),
    BinaryExpression(BinaryExpression),
    LogicalExpression(LogicalExpression),
    UnaryExpression(UnaryExpression),
    ConditionalExpression(ConditionalExpression),
    CallExpression(CallExpression),
    ArrayExpression(ArrayExpression),
    ObjectExpression(ObjectExpression),
    MemberExpression(MemberExpression),
}

/// Trait that all AST node types implement
pub trait ExpressionTrait: std::fmt::Display {
    fn span(&self) -> Option<Span>;

    /// Get the left and right binding power of this expression.
    /// When there is ambiguity in associativity, the expression with the lower binding power
    /// must be parenthesized
    fn binding_power(&self) -> (f64, f64) {
        (1000.0, 1000.0)
    }
}

impl Expression {
    pub fn clear_spans(&mut self) {
        let mut visitor = ClearSpanVisitor::new();
        self.walk_mut(&mut visitor);
    }

    /// Walk visitor through the expression tree in a DFS traversal
    pub fn walk(&self, visitor: &mut dyn ExpressionVisitor) {
        match self {
            Expression::BinaryExpression(node) => {
                node.left.walk(visitor);
                node.right.walk(visitor);
                visitor.visit_binary(node);
            }
            Expression::LogicalExpression(node) => {
                node.left.walk(visitor);
                node.right.walk(visitor);
                visitor.visit_logical(node);
            }
            Expression::UnaryExpression(node) => {
                node.argument.walk(visitor);
                visitor.visit_unary(node);
            }
            Expression::ConditionalExpression(node) => {
                node.test.walk(visitor);
                node.consequent.walk(visitor);
                node.alternate.walk(visitor);
                visitor.visit_conditional(node);
            }
            Expression::Literal(node) => {
                visitor.visit_literal(node);
            }
            Expression::Identifier(node) => {
                visitor.visit_identifier(node);
            }
            Expression::CallExpression(node) => {
                match &node.callee {
                    Callee::Identifier(ref identifier) => {
                        visitor.visit_called_identifier(identifier, &node.arguments)
                    }
                }
                for arg in &node.arguments {
                    arg.walk(visitor);
                }
                visitor.visit_call(node);
            }
            Expression::ArrayExpression(node) => {
                for el in &node.elements {
                    el.walk(visitor);
                }
                visitor.visit_array(node);
            }
            Expression::ObjectExpression(node) => {
                for prop in &node.properties {
                    visitor.visit_object_key(&prop.key);
                    prop.value.walk(visitor);
                }
                visitor.visit_object(node);
            }
            Expression::MemberExpression(node) => {
                node.object.walk(visitor);
                if node.computed {
                    node.property.walk(visitor);
                } else if let Expression::Identifier(identifier) = node.property.as_ref() {
                    visitor.visit_static_member_identifier(identifier);
                }
                visitor.visit_member(node);
            }
        }
    }

    pub fn walk_mut(&mut self, visitor: &mut dyn MutExpressionVisitor) {
        match self {
            Expression::BinaryExpression(node) => {
                node.left.walk_mut(visitor);
                node.right.walk_mut(visitor);
                visitor.visit_binary(node);
            }
            Expression::LogicalExpression(node) => {
                node.left.walk_mut(visitor);
                node.right.walk_mut(visitor);
                visitor.visit_logical(node);
            }
            Expression::UnaryExpression(node) => {
                node.argument.walk_mut(visitor);
                visitor.visit_unary(node);
            }
            Expression::ConditionalExpression(node) => {
                node.test.walk_mut(visitor);
                node.consequent.walk_mut(visitor);
                node.alternate.walk_mut(visitor);
                visitor.visit_conditional(node);
            }
            Expression::Literal(node) => {
                visitor.visit_literal(node);
            }
            Expression::Identifier(node) => {
                visitor.visit_identifier(node);
            }
            Expression::CallExpression(node) => {
                match &mut node.callee {
                    Callee::Identifier(ref mut identifier) => {
                        visitor.visit_called_identifier(identifier, &mut node.arguments)
                    }
                }
                for arg in &mut node.arguments {
                    arg.walk_mut(visitor);
                }
                visitor.visit_call(node);
            }
            Expression::ArrayExpression(node) => {
                for el in &mut node.elements {
                    el.walk_mut(visitor);
                }
                visitor.visit_array(node);
            }
            Expression::ObjectExpression(node) => {
                for prop in &mut node.properties {
                    visitor.visit_object_key(&mut prop.key);
                    prop.value.walk_mut(visitor);
                }
                visitor.visit_object(node);
            }
            Expression::MemberExpression(node) => {
                node.object.walk_mut(visitor);
                if node.computed {
                    node.property.walk_mut(visitor);
                } else if let Expression::Identifier(identifier) = node.property.as_mut() {
                    visitor.visit_static_member_identifier(identifier);
                }
                visitor.visit_member(node);
            }
        }
    }
}

impl Deref for Expression {
    type Target = dyn ExpressionTrait;

    fn deref(&self) -> &Self::Target {
        match self {
            Expression::Identifier(node) => node,
            Expression::Literal(node) => node,
            Expression::BinaryExpression(node) => node,
            Expression::LogicalExpression(node) => node,
            Expression::UnaryExpression(node) => node,
            Expression::ConditionalExpression(node) => node,
            Expression::CallExpression(node) => node,
            Expression::ArrayExpression(node) => node,
            Expression::ObjectExpression(node) => node,
            Expression::MemberExpression(node) => node,
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.deref().to_string())
    }
}

impl From<Identifier> for Expression {
    fn from(node: Identifier) -> Self {
        Self::Identifier(node)
    }
}

impl From<Literal> for Expression {
    fn from(node: Literal) -> Self {
        Self::Literal(node)
    }
}

impl From<BinaryExpression> for Expression {
    fn from(node: BinaryExpression) -> Self {
        Self::BinaryExpression(node)
    }
}

impl From<LogicalExpression> for Expression {
    fn from(node: LogicalExpression) -> Self {
        Self::LogicalExpression(node)
    }
}

impl From<UnaryExpression> for Expression {
    fn from(node: UnaryExpression) -> Self {
        Self::UnaryExpression(node)
    }
}

impl From<ConditionalExpression> for Expression {
    fn from(node: ConditionalExpression) -> Self {
        Self::ConditionalExpression(node)
    }
}

impl From<CallExpression> for Expression {
    fn from(node: CallExpression) -> Self {
        Self::CallExpression(node)
    }
}

impl From<ArrayExpression> for Expression {
    fn from(node: ArrayExpression) -> Self {
        Self::ArrayExpression(node)
    }
}

impl From<ObjectExpression> for Expression {
    fn from(node: ObjectExpression) -> Self {
        Self::ObjectExpression(node)
    }
}

impl From<MemberExpression> for Expression {
    fn from(node: MemberExpression) -> Self {
        Self::MemberExpression(node)
    }
}
