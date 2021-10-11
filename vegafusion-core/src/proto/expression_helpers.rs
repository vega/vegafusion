use std::fmt::{Display, Formatter};
use crate::proto::gen::expression::{Expression, Identifier, Literal, UnaryExpression, UnaryOperator, BinaryOperator, BinaryExpression, LogicalOperator, LogicalExpression, ConditionalExpression, CallExpression, ArrayExpression, ObjectExpression, Property, literal, MemberExpression};
use crate::proto::gen::expression::expression::Expr;
use crate::proto::gen::expression::literal::Value;
use std::ops::Deref;
use crate::proto::gen::expression::property::Key;
use std::collections::{HashSet, HashMap};
use itertools::sorted;
use crate::variable::Variable;


/// Trait that all AST node types implement
pub trait ExpressionTrait: Display {
    /// Get the left and right binding power of this expression.
    /// When there is ambiguity in associativity, the expression with the lower binding power
    /// must be parenthesized
    fn binding_power(&self) -> (f64, f64) {
        (1000.0, 1000.0)
    }
}

// Identifier
impl ExpressionTrait for Identifier {}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}


// Literal
impl Display for literal::Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Boolean(v) => {
                write!(f, "{}", v)
            }
            Value::Number(v) => {
                write!(f, "{}", v)
            }
            Value::String(v) => {
                write!(f, "\"{}\"", v)
            }
            Value::Null(v) => {
                write!(f, "null")
            }
        }
    }
}

impl ExpressionTrait for Literal {}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(value) = &self.value {
            write!(f, "{}", value)
        } else {
            write!(f, "None")
        }
    }
}

// Unary
impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Pos => write!(f, "+"),
            UnaryOperator::Neg => write!(f, "-"),
            UnaryOperator::Not => write!(f, "!"),
        }
    }
}

impl UnaryOperator {
    pub fn unary_binding_power(&self) -> f64 {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        match self {
            UnaryOperator::Neg | UnaryOperator::Pos | UnaryOperator::Not => 17.0,
        }
    }
}

impl UnaryExpression {
    pub fn unary_binding_power(&self) -> f64 {
        self.to_operator().unary_binding_power()
    }

    pub fn to_operator(&self) -> UnaryOperator {
        UnaryOperator::from_i32(self.operator).unwrap()
    }
}

impl ExpressionTrait for UnaryExpression {
    fn binding_power(&self) -> (f64, f64) {
        let power = self.unary_binding_power();
        (power, power)
    }
}

impl Display for UnaryExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (_, arg_right_bp) = self.argument.as_ref().unwrap().deref().binding_power();
        let (self_left_bp, _) = self.binding_power();
        let op = self.to_operator();
        if self_left_bp > arg_right_bp {
            write!(f, "{}({})", op, self.argument.as_ref().unwrap())
        } else {
            write!(f, "{}{}", op, self.argument.as_ref().unwrap())
        }
    }
}

// Binary
impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Mult => write!(f, "*"),
            BinaryOperator::Div => write!(f, "/"),
            BinaryOperator::Mod => write!(f, "%"),
            BinaryOperator::Equals => write!(f, "=="),
            BinaryOperator::StrictEquals => write!(f, "==="),
            BinaryOperator::NotEquals => write!(f, "!="),
            BinaryOperator::NotStrictEquals => write!(f, "!=="),
            BinaryOperator::GreaterThan => write!(f, ">"),
            BinaryOperator::LessThan => write!(f, "<"),
            BinaryOperator::GreaterThanEqual => write!(f, ">="),
            BinaryOperator::LessThanEqual => write!(f, "<="),
        }
    }
}

impl BinaryOperator {
    pub fn infix_binding_power(&self) -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left

        match self {
            BinaryOperator::Plus | BinaryOperator::Minus => (14.0, 14.5),
            BinaryOperator::Mult | BinaryOperator::Div | BinaryOperator::Mod => (15.0, 15.5),
            BinaryOperator::GreaterThan
            | BinaryOperator::LessThan
            | BinaryOperator::GreaterThanEqual
            | BinaryOperator::LessThanEqual => (12.0, 12.5),
            BinaryOperator::Equals
            | BinaryOperator::StrictEquals
            | BinaryOperator::NotEquals
            | BinaryOperator::NotStrictEquals => (11.0, 11.5),
        }
    }
}


impl BinaryExpression {
    pub fn to_operator(&self) -> BinaryOperator {
        BinaryOperator::from_i32(self.operator).unwrap()
    }

    pub fn infix_binding_power(&self) -> (f64, f64) {
        self.to_operator().infix_binding_power()
    }
}

impl ExpressionTrait for BinaryExpression {
    fn binding_power(&self) -> (f64, f64) {
        self.infix_binding_power()
    }
}

impl Display for BinaryExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Use binding power to determine whether lhs and/or rhs need parens
        let lhs_bp = self.left.as_ref().unwrap().binding_power();
        let rhs_bp = self.right.as_ref().unwrap().binding_power();
        let self_bp = self.binding_power();
        let lhs_parens = lhs_bp.1 < self_bp.0;
        let rhs_parens = rhs_bp.0 < self_bp.1;

        // Write lhs
        if lhs_parens {
            write!(f, "({})", self.left.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.left.as_ref().unwrap())?;
        }

        write!(f, " {} ", self.to_operator())?;

        // Write rhs
        if rhs_parens {
            write!(f, "({})", self.right.as_ref().unwrap())
        } else {
            write!(f, "{}", self.right.as_ref().unwrap())
        }
    }
}

// Logical
impl Display for LogicalOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalOperator::Or => write!(f, "||"),
            LogicalOperator::And => write!(f, "&&"),
        }
    }
}

impl LogicalOperator {
    pub fn infix_binding_power(&self) -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left
        match self {
            LogicalOperator::Or => (6.0, 6.5),
            LogicalOperator::And => (7.0, 7.5),
        }
    }
}

impl LogicalExpression {
    pub fn to_operator(&self) -> LogicalOperator {
        LogicalOperator::from_i32(self.operator).unwrap()
    }

    pub fn infix_binding_power(&self) -> (f64, f64) {
        self.to_operator().infix_binding_power()
    }
}

impl ExpressionTrait for LogicalExpression {
    fn binding_power(&self) -> (f64, f64) {
        self.infix_binding_power()
    }
}

impl Display for LogicalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Use binding power to determine whether lhs and/or rhs need parens
        let lhs_bp = self.left.as_ref().unwrap().binding_power();
        let rhs_bp = self.right.as_ref().unwrap().binding_power();
        let self_bp = self.binding_power();
        let lhs_parens = lhs_bp.1 < self_bp.0;
        let rhs_parens = rhs_bp.0 < self_bp.1;

        // Write lhs
        if lhs_parens {
            write!(f, "({})", self.left.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.left.as_ref().unwrap())?;
        }

        write!(f, " {} ", self.to_operator())?;

        // Write rhs
        if rhs_parens {
            write!(f, "({})", self.right.as_ref().unwrap())
        } else {
            write!(f, "{}", self.right.as_ref().unwrap())
        }
    }
}

// Conditional
impl ConditionalExpression {
    pub fn ternary_binding_power() -> (f64, f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        (4.8, 4.6, 4.4)
    }
}

impl ExpressionTrait for ConditionalExpression {
    fn binding_power(&self) -> (f64, f64) {
        let (left_bp, _, right_bp) = Self::ternary_binding_power();
        (left_bp, right_bp)
    }
}

impl Display for ConditionalExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (left_bp, middle_bp, right_bp) = Self::ternary_binding_power();
        // Write test expression using binding power to determine whether parens are needed
        let test_bp = self.test.as_ref().unwrap().binding_power();
        let consequent_bp = self.consequent.as_ref().unwrap().binding_power();
        let alternate_bp = self.alternate.as_ref().unwrap().binding_power();

        if test_bp.1 < left_bp {
            write!(f, "({})", self.test.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.test.as_ref().unwrap())?;
        }

        write!(f, " ? ")?;

        // Write consequent expression using binding power to determine whether parens are needed
        if consequent_bp.1 < middle_bp {
            write!(f, "({})", self.consequent.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.consequent.as_ref().unwrap())?;
        }

        write!(f, ": ")?;

        if alternate_bp.0 < right_bp {
            write!(f, "({})", self.alternate.as_ref().unwrap())
        } else {
            write!(f, "{}", self.alternate.as_ref().unwrap())
        }
    }
}

// Call
impl ExpressionTrait for CallExpression {}

impl Display for CallExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let internal_binding_power = 1.0;
        let mut arg_strings: Vec<String> = Vec::new();
        for arg in self.arguments.iter() {
            let arg_binding_power = arg.binding_power().0;
            let arg_string = if arg_binding_power > internal_binding_power {
                format!("{}", arg)
            } else {
                // e.g. the argument is a comma infix operation, so it must be wrapped in parens
                format!("({})", arg)
            };
            arg_strings.push(arg_string)
        }
        let args_string = arg_strings.join(", ");

        write!(f, "{}({})", self.callee, args_string)
    }
}

// Array
impl ExpressionTrait for ArrayExpression {}

impl Display for ArrayExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

// Object
impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Literal(v) => write!(f, "{}", v.value.as_ref().unwrap()),
            Key::Identifier(v) => write!(f, "{}", v),
        }
    }
}

impl Key {
    // Get string for property for use as object key. Strings should not be quoted
    pub fn to_object_key_string(&self) -> String {
        match self {
            Key::Literal(v) => match v.value.as_ref().unwrap() {
                Value::String(s) => s.clone(),
                _ => v.to_string(),
            },
            Key::Identifier(v) => v.name.clone(),
        }
    }
}

impl Display for Property {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key.as_ref().unwrap(), self.value.as_ref().unwrap())
    }
}

impl ExpressionTrait for ObjectExpression {}

impl Display for ObjectExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let property_strings: Vec<String> = self.properties.iter().map(|p| p.to_string()).collect();
        let property_csv = property_strings.join(", ");
        write!(f, "{{{}}}", property_csv)
    }
}

// Member
impl MemberExpression {
    pub fn member_binding_power() -> (f64, f64) {
        // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
        //  - left-to-right operators have larger number to the right
        //  - right-to-left have larger number to the left
        (20.0, 20.5)
    }
}

impl ExpressionTrait for MemberExpression {
    fn binding_power(&self) -> (f64, f64) {
        Self::member_binding_power()
    }
}

impl Display for MemberExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let object_rhs_bp = self.object.as_ref().unwrap().binding_power().1;

        // Write object, using binding power to determine whether to wrap it in parenthesis
        if object_rhs_bp < Self::member_binding_power().0 {
            write!(f, "({})", self.object.as_ref().unwrap())?;
        } else {
            write!(f, "{}", self.object.as_ref().unwrap())?;
        }

        // Write property
        if self.computed {
            write!(f, "[{}]", self.property.as_ref().unwrap())
        } else {
            write!(f, ".{}", self.property.as_ref().unwrap())
        }
    }
}

// Base expression
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
    // pub fn clear_spans(&mut self) {
    //     let mut visitor = ClearSpansVisitor::new();
    //     self.walk_mut(&mut visitor);
    // }

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
                if node.computed {
                    node.property.as_ref().unwrap().walk(visitor);
                } else if let Expr::Identifier(identifier) = prop_expr {
                    visitor.visit_static_member_identifier(identifier);
                }
                visitor.visit_member(node);
            }
        }
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


pub trait ExpressionVisitor {
    fn visit_identifier(&mut self, _node: &Identifier) {}
    fn visit_called_identifier(&mut self, _node: &Identifier, _args: &Vec<Expression>) {}
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
//
// pub trait MutExpressionVisitor {
//     fn visit_identifier(&mut self, _node: &mut Identifier) {}
//     fn visit_called_identifier(&mut self, _node: &mut Identifier, _args: &mut Vec<Expression>) {}
//     fn visit_literal(&mut self, _node: &mut Literal) {}
//     fn visit_binary(&mut self, _node: &mut BinaryExpression) {}
//     fn visit_logical(&mut self, _node: &mut LogicalExpression) {}
//     fn visit_unary(&mut self, _node: &mut UnaryExpression) {}
//     fn visit_conditional(&mut self, _node: &mut ConditionalExpression) {}
//     fn visit_member(&mut self, _node: &mut MemberExpression) {}
//     fn visit_call(&mut self, _node: &mut CallExpression) {}
//     fn visit_array(&mut self, _node: &mut ArrayExpression) {}
//     fn visit_object(&mut self, _node: &mut ObjectExpression) {}
//     fn visit_object_key(&mut self, _node: &mut PropertyKey) {}
//     fn visit_static_member_identifier(&mut self, _node: &mut Identifier) {}
// }
//
// /// Visitor to set all spans in the expression tree to None
// #[derive(Clone, Default)]
// pub struct ClearSpansVisitor {}
// impl ClearSpansVisitor {
//     pub fn new() -> Self {
//         Self {}
//     }
// }
//
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

