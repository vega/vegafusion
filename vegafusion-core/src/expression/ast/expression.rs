use std::fmt::{Display, Formatter};
use crate::proto::gen::expression::{Expression, Identifier};
use std::ops::Deref;
use crate::proto::gen::expression::expression::Expr;
use crate::variable::Variable;
use crate::expression::visitors::{GetVariablesVisitor, ExpressionVisitor, MutExpressionVisitor, ClearSpansVisitor};
use itertools::sorted;

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
                node.left.as_ref().unwrap().walk(visitor);
                node.right.as_ref().unwrap().walk(visitor);
                visitor.visit_binary(node);
            }
            Expr::Logical(node) => {
                node.left.as_ref().unwrap().walk(visitor);
                node.right.as_ref().unwrap().walk(visitor);
                visitor.visit_logical(node);
            }
            Expr::Unary(node) => {
                node.argument.as_ref().unwrap().walk(visitor);
                visitor.visit_unary(node);
            }
            Expr::Conditional(node) => {
                node.test.as_ref().unwrap().walk(visitor);
                node.consequent.as_ref().unwrap().walk(visitor);
                node.alternate.as_ref().unwrap().walk(visitor);
                visitor.visit_conditional(node);
            }
            Expr::Literal(node) => {
                visitor.visit_literal(node);
            }
            Expr::Identifier(node) => {
                visitor.visit_identifier(node);
            }
            Expr::Call(node) => {
                let callee_id = Identifier {name: node.callee.clone()};
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
                let mut callee_id = Identifier {name: node.callee.clone()};
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

    // pub fn walk_mut(&mut self, visitor: &mut dyn MutExpressionVisitor) {
    //     match self {
    //         Expression::BinaryExpression(node) => {
    //             node.left.walk_mut(visitor);
    //             node.right.walk_mut(visitor);
    //             visitor.visit_binary(node);
    //         }
    //         Expression::LogicalExpression(node) => {
    //             node.left.walk_mut(visitor);
    //             node.right.walk_mut(visitor);
    //             visitor.visit_logical(node);
    //         }
    //         Expression::UnaryExpression(node) => {
    //             node.argument.walk_mut(visitor);
    //             visitor.visit_unary(node);
    //         }
    //         Expression::ConditionalExpression(node) => {
    //             node.test.walk_mut(visitor);
    //             node.consequent.walk_mut(visitor);
    //             node.alternate.walk_mut(visitor);
    //             visitor.visit_conditional(node);
    //         }
    //         Expression::Literal(node) => {
    //             visitor.visit_literal(node);
    //         }
    //         Expression::Identifier(node) => {
    //             visitor.visit_identifier(node);
    //         }
    //         Expression::CallExpression(node) => {
    //             match &mut node.callee {
    //                 Callee::Identifier(ref mut identifier) => {
    //                     visitor.visit_called_identifier(identifier, &mut node.arguments)
    //                 }
    //             }
    //             for arg in &mut node.arguments {
    //                 arg.walk_mut(visitor);
    //             }
    //             visitor.visit_call(node);
    //         }
    //         Expression::ArrayExpression(node) => {
    //             for el in &mut node.elements {
    //                 el.walk_mut(visitor);
    //             }
    //             visitor.visit_array(node);
    //         }
    //         Expression::ObjectExpression(node) => {
    //             for prop in &mut node.properties {
    //                 visitor.visit_object_key(&mut prop.key);
    //                 prop.value.walk_mut(visitor);
    //             }
    //             visitor.visit_object(node);
    //         }
    //         Expression::MemberExpression(node) => {
    //             node.object.walk_mut(visitor);
    //             if node.computed {
    //                 node.property.walk_mut(visitor);
    //             } else if let Expression::Identifier(identifier) = node.property.as_mut() {
    //                 visitor.visit_static_member_identifier(identifier);
    //             }
    //             visitor.visit_member(node);
    //         }
    //     }
    // }
}
