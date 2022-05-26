/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, VegaFusionError};
use crate::expression::column_usage::{
    DatasetsColumnUsage, GetDatasetsColumnUsage, VlSelectionFields,
};
use crate::expression::visitors::{
    CheckSupportedExprVisitor, ClearSpansVisitor, DatasetsColumnUsageVisitor, ExpressionVisitor,
    GetInputVariablesVisitor, ImplicitVariablesExprVisitor, MutExpressionVisitor,
    UpdateVariablesExprVisitor,
};
use crate::proto::gen::expression::expression::Expr;
use crate::proto::gen::expression::{
    literal, ArrayExpression, BinaryExpression, CallExpression, ConditionalExpression, Expression,
    Identifier, Literal, LogicalExpression, MemberExpression, ObjectExpression, Span,
    UnaryExpression,
};
use crate::proto::gen::tasks::Variable;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use itertools::sorted;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

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

    pub fn input_vars(&self) -> Vec<InputVariable> {
        let mut visitor = GetInputVariablesVisitor::new();
        self.walk(&mut visitor);

        sorted(visitor.input_variables).collect()
    }

    pub fn update_vars(&self) -> Vec<Variable> {
        let mut visitor = UpdateVariablesExprVisitor::new();
        self.walk(&mut visitor);

        sorted(visitor.update_variables).collect()
    }

    pub fn implicit_vars(&self) -> Vec<String> {
        let mut visitor = ImplicitVariablesExprVisitor::new();
        self.walk(&mut visitor);
        sorted(visitor.implicit_vars).collect()
    }

    pub fn is_supported(&self) -> bool {
        let mut visitor = CheckSupportedExprVisitor::new();
        self.walk(&mut visitor);
        visitor.supported
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
                } else {
                    node.property.as_ref().unwrap().walk(visitor);
                }
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
                } else {
                    node.property.as_mut().unwrap().walk_mut(visitor);
                }
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

// Expression from literal
impl<V: Into<literal::Value>> From<V> for Expression {
    fn from(v: V) -> Self {
        Self {
            expr: Some(Expr::from(v)),
            span: None,
        }
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

impl<V: Into<literal::Value>> From<V> for Expr {
    fn from(v: V) -> Self {
        let v = v.into();
        let repr = v.to_string();
        Self::Literal(Literal::new(v, &repr))
    }
}

impl GetDatasetsColumnUsage for Expression {
    fn datasets_column_usage(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut visitor = DatasetsColumnUsageVisitor::new(
            datum_var,
            usage_scope,
            task_scope,
            vl_selection_fields,
        );
        self.walk(&mut visitor);
        visitor.dataset_column_usage
    }
}
