use crate::proto::gen::expression::property::Key;
use crate::proto::gen::expression::{
    ArrayExpression, BinaryExpression, CallExpression, ConditionalExpression, Expression,
    Identifier, Literal, LogicalExpression, MemberExpression, ObjectExpression, UnaryExpression,
};

use std::collections::HashSet;
use crate::proto::gen::tasks::Variable;

pub trait ExpressionVisitor {
    fn visit_expression(&mut self, _expression: &Expression) {}
    fn visit_identifier(&mut self, _node: &Identifier) {}
    fn visit_called_identifier(&mut self, _node: &Identifier, _args: &[Expression]) {}
    fn visit_literal(&mut self, _node: &Literal) {}
    fn visit_binary(&mut self, _node: &BinaryExpression) {}
    fn visit_logical(&mut self, _node: &LogicalExpression) {}
    fn visit_unary(&mut self, _node: &UnaryExpression) {}
    fn visit_conditional(&mut self, _node: &ConditionalExpression) {}
    fn visit_member(&mut self, _node: &MemberExpression) {}
    fn visit_call(&mut self, _node: &CallExpression) {}
    fn visit_array(&mut self, _node: &ArrayExpression) {}
    fn visit_object(&mut self, _node: &ObjectExpression) {}
    fn visit_object_key(&mut self, _node: &Key) {}
    fn visit_static_member_identifier(&mut self, _node: &Identifier) {}
}

pub trait MutExpressionVisitor {
    fn visit_expression(&mut self, _expression: &mut Expression) {}
    fn visit_identifier(&mut self, _node: &mut Identifier) {}
    fn visit_called_identifier(&mut self, _node: &mut Identifier, _args: &mut [Expression]) {}
    fn visit_literal(&mut self, _node: &mut Literal) {}
    fn visit_binary(&mut self, _node: &mut BinaryExpression) {}
    fn visit_logical(&mut self, _node: &mut LogicalExpression) {}
    fn visit_unary(&mut self, _node: &mut UnaryExpression) {}
    fn visit_conditional(&mut self, _node: &mut ConditionalExpression) {}
    fn visit_member(&mut self, _node: &mut MemberExpression) {}
    fn visit_call(&mut self, _node: &mut CallExpression) {}
    fn visit_array(&mut self, _node: &mut ArrayExpression) {}
    fn visit_object(&mut self, _node: &mut ObjectExpression) {}
    fn visit_object_key(&mut self, _node: &mut Key) {}
    fn visit_static_member_identifier(&mut self, _node: &mut Identifier) {}
}

/// Visitor to set all spans in the expression tree to None
#[derive(Clone, Default)]
pub struct ClearSpansVisitor {}
impl ClearSpansVisitor {
    pub fn new() -> Self {
        Self {}
    }
}

impl MutExpressionVisitor for ClearSpansVisitor {
    fn visit_expression(&mut self, expression: &mut Expression) {
        expression.span.take();
    }
}

// impl MutExpressionVisitor for ClearSpansVisitor {
//     fn visit_identifier(&mut self, node: &mut Identifier) {
//         node.span.take();
//     }
//     fn visit_called_identifier(&mut self, node: &mut Identifier, _args: &mut Vec<Expression>) {
//         node.span.take();
//     }
//     fn visit_literal(&mut self, node: &mut Literal) {
//         node.span.take();
//     }
//     fn visit_binary(&mut self, node: &mut BinaryExpression) {
//         node.span.take();
//     }
//     fn visit_logical(&mut self, node: &mut LogicalExpression) {
//         node.span.take();
//     }
//     fn visit_unary(&mut self, node: &mut UnaryExpression) {
//         node.span.take();
//     }
//     fn visit_conditional(&mut self, node: &mut ConditionalExpression) {
//         node.span.take();
//     }
//     fn visit_member(&mut self, node: &mut MemberExpression) {
//         node.span.take();
//     }
//     fn visit_call(&mut self, node: &mut CallExpression) {
//         node.span.take();
//     }
//     fn visit_array(&mut self, node: &mut ArrayExpression) {
//         node.span.take();
//     }
//     fn visit_object(&mut self, node: &mut ObjectExpression) {
//         node.span.take();
//     }
//     fn visit_object_key(&mut self, node: &mut PropertyKey) {
//         match node {
//             PropertyKey::Literal(node) => {
//                 node.span.take();
//             }
//             PropertyKey::Identifier(node) => {
//                 node.span.take();
//             }
//         }
//     }
//     fn visit_static_member_identifier(&mut self, node: &mut Identifier) {
//         node.span.take();
//     }
// }

/// Visitor to collect all unbound variables in the expression
#[derive(Clone, Default)]
pub struct GetVariablesVisitor {
    pub variables: HashSet<Variable>,
    // pub callables: HashMap<String, VegaFusionCallable>,
}
impl GetVariablesVisitor {
    pub fn new() -> Self {
        Self {
            variables: Default::default(),
            // callables: default_callables(),
        }
    }
}

impl ExpressionVisitor for GetVariablesVisitor {
    fn visit_identifier(&mut self, node: &Identifier) {
        // datum does not count as a variable
        if node.name != "datum" {
            self.variables.insert(Variable::new_signal(&node.name));
        }
    }

    // /// Collect data and scale identifiers. These show up as a literal string as the first
    // /// argument to a Data or Scale callable.
    // fn visit_called_identifier(&mut self, node: &Identifier, args: &Vec<Expression>) {
    //     if let Some(callable) = self.callables.get(&node.name) {
    //         if let Some(Expression::Literal(Literal {
    //                                             value: LiteralValue::String(arg),
    //                                             ..
    //                                         })) = args.get(0)
    //         {
    //             match callable {
    //                 VegaFusionCallable::Data => {
    //                     self.variables.insert(Variable::new_data(arg));
    //                 }
    //                 VegaFusionCallable::Scale => {
    //                     self.variables.insert(Variable::new_scale(arg));
    //                 }
    //                 _ => {}
    //             }
    //         }
    //     }
    // }
}
