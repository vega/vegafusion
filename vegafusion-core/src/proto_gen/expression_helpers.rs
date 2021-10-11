use std::fmt::{Display, Formatter};
use crate::proto_gen::expression::{Expression, Identifier, Literal, UnaryExpression, UnaryOperator, BinaryOperator, BinaryExpression, LogicalOperator, LogicalExpression, ConditionalExpression, CallExpression, ArrayExpression, ObjectExpression, Property, literal, MemberExpression};
use crate::proto_gen::expression::expression::Expr;
use crate::proto_gen::expression::literal::Value;
use std::ops::Deref;
use crate::proto_gen::expression::property::Key;


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