/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use serde::{Deserialize, Serialize};
use vegafusion_core::proto::gen::expression as proto_expression;

/// ESTree-style AST Node for identifiers
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#identifier
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct Identifier {
    pub name: String,
}

impl Identifier {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Identifier(proto_expression::Identifier {
            name: self.name.clone(),
        })
    }
}

/// AST Node for liter scalar values
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    String(String),
    Boolean(bool),
    Number(f64),
    Null,
}

impl LiteralValue {
    pub fn to_proto(&self) -> proto_expression::literal::Value {
        match self {
            LiteralValue::String(v) => proto_expression::literal::Value::String(v.clone()),
            LiteralValue::Boolean(v) => proto_expression::literal::Value::Boolean(*v),
            LiteralValue::Number(v) => proto_expression::literal::Value::Number(*v),
            LiteralValue::Null => proto_expression::literal::Value::Null(false),
        }
    }
}

/// ESTree-style AST Node for literal value
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#literal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Literal {
    pub value: LiteralValue,
    pub raw: String,
}

impl Literal {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Literal(proto_expression::Literal {
            raw: self.raw.clone(),
            value: Some(self.value.to_proto()),
        })
    }
}

/// ESTree-style AST Node for binary infix operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#binaryoperator
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum BinaryOperator {
    #[serde(rename = "==")]
    Equals,

    #[serde(rename = "!=")]
    NotEquals,

    #[serde(rename = "===")]
    StrictEquals,

    #[serde(rename = "!==")]
    NotStrictEquals,

    #[serde(rename = "<")]
    LessThan,

    #[serde(rename = "<=")]
    LessThanEqual,

    #[serde(rename = ">")]
    GreaterThan,

    #[serde(rename = ">=")]
    GreaterThanEqual,

    #[serde(rename = "+")]
    Plus,

    #[serde(rename = "-")]
    Minus,

    #[serde(rename = "*")]
    Mult,

    #[serde(rename = "/")]
    Div,

    #[serde(rename = "%")]
    Mod,
}

impl BinaryOperator {
    pub fn to_proto(&self) -> proto_expression::BinaryOperator {
        match self {
            BinaryOperator::Equals => proto_expression::BinaryOperator::Equals,
            BinaryOperator::NotEquals => proto_expression::BinaryOperator::NotEquals,
            BinaryOperator::StrictEquals => proto_expression::BinaryOperator::StrictEquals,
            BinaryOperator::NotStrictEquals => proto_expression::BinaryOperator::NotStrictEquals,
            BinaryOperator::LessThan => proto_expression::BinaryOperator::LessThan,
            BinaryOperator::LessThanEqual => proto_expression::BinaryOperator::LessThanEqual,
            BinaryOperator::GreaterThan => proto_expression::BinaryOperator::GreaterThan,
            BinaryOperator::GreaterThanEqual => proto_expression::BinaryOperator::GreaterThanEqual,
            BinaryOperator::Plus => proto_expression::BinaryOperator::Plus,
            BinaryOperator::Minus => proto_expression::BinaryOperator::Minus,
            BinaryOperator::Mult => proto_expression::BinaryOperator::Mult,
            BinaryOperator::Div => proto_expression::BinaryOperator::Div,
            BinaryOperator::Mod => proto_expression::BinaryOperator::Mod,
        }
    }
}

/// ESTree-style AST Node for binary infix expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#binaryexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryExpression {
    pub left: Box<ESTreeExpression>,
    pub operator: BinaryOperator,
    pub right: Box<ESTreeExpression>,
}

impl BinaryExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Binary(Box::new(proto_expression::BinaryExpression {
            left: Some(Box::new(self.left.to_proto())),
            operator: self.operator.to_proto() as i32,
            right: Some(Box::new(self.right.to_proto())),
        }))
    }
}

/// ESTree-style AST Node for logical infix operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#logicaloperator
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum LogicalOperator {
    #[serde(rename = "||")]
    Or,

    #[serde(rename = "&&")]
    And,
}

impl LogicalOperator {
    pub fn to_proto(&self) -> proto_expression::LogicalOperator {
        match self {
            LogicalOperator::Or => proto_expression::LogicalOperator::Or,
            LogicalOperator::And => proto_expression::LogicalOperator::And,
        }
    }
}

/// ESTree-style AST Node for logical infix expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#logicalexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalExpression {
    pub left: Box<ESTreeExpression>,
    pub operator: LogicalOperator,
    pub right: Box<ESTreeExpression>,
}

impl LogicalExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Logical(Box::new(proto_expression::LogicalExpression {
            left: Some(Box::new(self.left.to_proto())),
            operator: self.operator.to_proto() as i32,
            right: Some(Box::new(self.right.to_proto())),
        }))
    }
}

/// ESTree-style AST Node for unary operators
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#unaryoperator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnaryOperator {
    #[serde(rename = "+")]
    Pos,

    #[serde(rename = "-")]
    Neg,

    #[serde(rename = "!")]
    Not,
}

impl UnaryOperator {
    pub fn to_proto(&self) -> proto_expression::UnaryOperator {
        match self {
            UnaryOperator::Pos => proto_expression::UnaryOperator::Pos,
            UnaryOperator::Neg => proto_expression::UnaryOperator::Neg,
            UnaryOperator::Not => proto_expression::UnaryOperator::Not,
        }
    }
}

/// ESTree-style AST Node for unary expressions
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#updateexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnaryExpression {
    pub operator: UnaryOperator,
    pub prefix: bool,
    pub argument: Box<ESTreeExpression>,
}

impl UnaryExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Unary(Box::new(proto_expression::UnaryExpression {
            argument: Some(Box::new(self.argument.to_proto())),
            operator: self.operator.to_proto() as i32,
            prefix: self.prefix,
        }))
    }
}

/// ESTree-style AST Node for conditional/ternary expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#conditionalexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalExpression {
    pub test: Box<ESTreeExpression>,
    pub consequent: Box<ESTreeExpression>,
    pub alternate: Box<ESTreeExpression>,
}

impl ConditionalExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Conditional(Box::new(
            proto_expression::ConditionalExpression {
                test: Some(Box::new(self.test.to_proto())),
                consequent: Some(Box::new(self.consequent.to_proto())),
                alternate: Some(Box::new(self.alternate.to_proto())),
            },
        ))
    }
}

/// Enum that serializes like Expression, but only includes the Identifier variant
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Callee {
    Identifier(Identifier),
}

/// ESTree-style AST Node for call expression
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#callexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallExpression {
    pub callee: Callee,
    pub arguments: Vec<ESTreeExpression>,
}

impl CallExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        let callee_string = match &self.callee {
            Callee::Identifier(id) => id.name.clone(),
        };

        proto_expression::expression::Expr::Call(proto_expression::CallExpression {
            callee: callee_string,
            arguments: self.arguments.iter().map(|arg| arg.to_proto()).collect(),
        })
    }
}

/// ESTree-style AST Node for array literal (e.g. [23, "Hello"])
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#arrayexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayExpression {
    pub elements: Vec<ESTreeExpression>,
}

impl ArrayExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Array(proto_expression::ArrayExpression {
            elements: self.elements.iter().map(|e| e.to_proto()).collect(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PropertyKey {
    Literal(Literal),
    Identifier(Identifier),
}

impl PropertyKey {
    pub fn to_proto(&self) -> proto_expression::property::Key {
        match self {
            PropertyKey::Literal(lit) => {
                proto_expression::property::Key::Literal(proto_expression::Literal {
                    raw: lit.raw.clone(),
                    value: Some(lit.value.to_proto()),
                })
            }
            PropertyKey::Identifier(ident) => {
                proto_expression::property::Key::Identifier(proto_expression::Identifier {
                    name: ident.name.clone(),
                })
            }
        }
    }
}

/// ESTree-style AST Node for object property (e.g. `a: 23`)
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#property
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Property {
    pub key: PropertyKey,
    pub value: ESTreeExpression,
    pub kind: String,
}

impl Property {
    pub fn to_proto(&self) -> proto_expression::Property {
        proto_expression::Property {
            value: Some(self.value.to_proto()),
            kind: self.kind.clone(),
            key: Some(self.key.to_proto()),
        }
    }
}

/// ESTree-style AST Node for object literal (e.g. `{a: 23, "Hello": false}`)
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#objectexpression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectExpression {
    pub properties: Vec<Property>,
}

impl ObjectExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Object(proto_expression::ObjectExpression {
            properties: self.properties.iter().map(|p| p.to_proto()).collect(),
        })
    }
}

/// ESTree-style AST Node for member/element lookup in object or array (e.g. `foo[bar]`))
///
/// https://github.com/estree/estree/blob/0fa6c005fa452f1f970b3923d5faa38178906d08/es5.md#property
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberExpression {
    pub object: Box<ESTreeExpression>,
    pub property: Box<ESTreeExpression>,
    pub computed: bool,
}

impl MemberExpression {
    pub fn to_proto(&self) -> proto_expression::expression::Expr {
        proto_expression::expression::Expr::Member(Box::new(proto_expression::MemberExpression {
            object: Some(Box::new(self.object.to_proto())),
            property: Some(Box::new(self.property.to_proto())),
            computed: self.computed,
        }))
    }
}

/// Expression enum that serializes to ESTree compatible JSON object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ESTreeExpression {
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

impl ESTreeExpression {
    pub fn to_proto(&self) -> proto_expression::Expression {
        let expr = match self {
            ESTreeExpression::Identifier(expr) => expr.to_proto(),
            ESTreeExpression::Literal(expr) => expr.to_proto(),
            ESTreeExpression::BinaryExpression(expr) => expr.to_proto(),
            ESTreeExpression::LogicalExpression(expr) => expr.to_proto(),
            ESTreeExpression::UnaryExpression(expr) => expr.to_proto(),
            ESTreeExpression::ConditionalExpression(expr) => expr.to_proto(),
            ESTreeExpression::CallExpression(expr) => expr.to_proto(),
            ESTreeExpression::ArrayExpression(expr) => expr.to_proto(),
            ESTreeExpression::ObjectExpression(expr) => expr.to_proto(),
            ESTreeExpression::MemberExpression(expr) => expr.to_proto(),
        };

        proto_expression::Expression {
            span: None,
            expr: Some(expr),
        }
    }
}
