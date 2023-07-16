/// ESTree-style AST nodes
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(int32, tag = "1")]
    pub start: i32,
    #[prost(int32, tag = "2")]
    pub end: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Literal {
    #[prost(string, tag = "1")]
    pub raw: ::prost::alloc::string::String,
    #[prost(oneof = "literal::Value", tags = "2, 3, 4, 5")]
    pub value: ::core::option::Option<literal::Value>,
}
/// Nested message and enum types in `Literal`.
pub mod literal {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "2")]
        String(::prost::alloc::string::String),
        #[prost(bool, tag = "3")]
        Boolean(bool),
        #[prost(double, tag = "4")]
        Number(f64),
        #[prost(bool, tag = "5")]
        Null(bool),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IdentifierAbc {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Identifier {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnaryExpression {
    #[prost(enumeration = "UnaryOperator", tag = "1")]
    pub operator: i32,
    #[prost(bool, tag = "2")]
    pub prefix: bool,
    #[prost(message, optional, boxed, tag = "3")]
    pub argument: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExpression {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(enumeration = "LogicalOperator", tag = "2")]
    pub operator: i32,
    #[prost(message, optional, boxed, tag = "3")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryExpression {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(enumeration = "BinaryOperator", tag = "2")]
    pub operator: i32,
    #[prost(message, optional, boxed, tag = "3")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConditionalExpression {
    #[prost(message, optional, boxed, tag = "1")]
    pub test: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub consequent: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub alternate: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberExpression {
    #[prost(message, optional, boxed, tag = "1")]
    pub object: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub property: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(bool, tag = "3")]
    pub computed: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayExpression {
    #[prost(message, repeated, tag = "1")]
    pub elements: ::prost::alloc::vec::Vec<Expression>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CallExpression {
    #[prost(string, tag = "1")]
    pub callee: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub arguments: ::prost::alloc::vec::Vec<Expression>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Property {
    #[prost(message, optional, tag = "3")]
    pub value: ::core::option::Option<Expression>,
    #[prost(string, tag = "4")]
    pub kind: ::prost::alloc::string::String,
    #[prost(oneof = "property::Key", tags = "1, 2")]
    pub key: ::core::option::Option<property::Key>,
}
/// Nested message and enum types in `Property`.
pub mod property {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Key {
        #[prost(message, tag = "1")]
        Literal(super::Literal),
        #[prost(message, tag = "2")]
        Identifier(super::Identifier),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectExpression {
    #[prost(message, repeated, tag = "1")]
    pub properties: ::prost::alloc::vec::Vec<Property>,
}
/// Top-level expression
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expression {
    #[prost(message, optional, tag = "11")]
    pub span: ::core::option::Option<Span>,
    #[prost(oneof = "expression::Expr", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub expr: ::core::option::Option<expression::Expr>,
}
/// Nested message and enum types in `Expression`.
pub mod expression {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Expr {
        #[prost(message, tag = "1")]
        Identifier(super::Identifier),
        #[prost(message, tag = "2")]
        Literal(super::Literal),
        #[prost(message, tag = "3")]
        Binary(::prost::alloc::boxed::Box<super::BinaryExpression>),
        #[prost(message, tag = "4")]
        Logical(::prost::alloc::boxed::Box<super::LogicalExpression>),
        #[prost(message, tag = "5")]
        Unary(::prost::alloc::boxed::Box<super::UnaryExpression>),
        #[prost(message, tag = "6")]
        Conditional(::prost::alloc::boxed::Box<super::ConditionalExpression>),
        #[prost(message, tag = "7")]
        Call(super::CallExpression),
        #[prost(message, tag = "8")]
        Array(super::ArrayExpression),
        #[prost(message, tag = "9")]
        Object(super::ObjectExpression),
        #[prost(message, tag = "10")]
        Member(::prost::alloc::boxed::Box<super::MemberExpression>),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UnaryOperator {
    Pos = 0,
    Neg = 1,
    Not = 2,
}
impl UnaryOperator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UnaryOperator::Pos => "Pos",
            UnaryOperator::Neg => "Neg",
            UnaryOperator::Not => "Not",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Pos" => Some(Self::Pos),
            "Neg" => Some(Self::Neg),
            "Not" => Some(Self::Not),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LogicalOperator {
    Or = 0,
    And = 1,
}
impl LogicalOperator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LogicalOperator::Or => "Or",
            LogicalOperator::And => "And",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Or" => Some(Self::Or),
            "And" => Some(Self::And),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BinaryOperator {
    Equals = 0,
    NotEquals = 1,
    StrictEquals = 2,
    NotStrictEquals = 3,
    LessThan = 4,
    LessThanEqual = 5,
    GreaterThan = 6,
    GreaterThanEqual = 7,
    Plus = 8,
    Minus = 9,
    Mult = 10,
    Div = 11,
    Mod = 12,
    BitwiseAnd = 13,
    BitwiseOr = 14,
    BitwiseXor = 15,
    BitwiseShiftLeft = 16,
    BitwiseShiftRight = 17,
}
impl BinaryOperator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BinaryOperator::Equals => "Equals",
            BinaryOperator::NotEquals => "NotEquals",
            BinaryOperator::StrictEquals => "StrictEquals",
            BinaryOperator::NotStrictEquals => "NotStrictEquals",
            BinaryOperator::LessThan => "LessThan",
            BinaryOperator::LessThanEqual => "LessThanEqual",
            BinaryOperator::GreaterThan => "GreaterThan",
            BinaryOperator::GreaterThanEqual => "GreaterThanEqual",
            BinaryOperator::Plus => "Plus",
            BinaryOperator::Minus => "Minus",
            BinaryOperator::Mult => "Mult",
            BinaryOperator::Div => "Div",
            BinaryOperator::Mod => "Mod",
            BinaryOperator::BitwiseAnd => "BitwiseAnd",
            BinaryOperator::BitwiseOr => "BitwiseOr",
            BinaryOperator::BitwiseXor => "BitwiseXor",
            BinaryOperator::BitwiseShiftLeft => "BitwiseShiftLeft",
            BinaryOperator::BitwiseShiftRight => "BitwiseShiftRight",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Equals" => Some(Self::Equals),
            "NotEquals" => Some(Self::NotEquals),
            "StrictEquals" => Some(Self::StrictEquals),
            "NotStrictEquals" => Some(Self::NotStrictEquals),
            "LessThan" => Some(Self::LessThan),
            "LessThanEqual" => Some(Self::LessThanEqual),
            "GreaterThan" => Some(Self::GreaterThan),
            "GreaterThanEqual" => Some(Self::GreaterThanEqual),
            "Plus" => Some(Self::Plus),
            "Minus" => Some(Self::Minus),
            "Mult" => Some(Self::Mult),
            "Div" => Some(Self::Div),
            "Mod" => Some(Self::Mod),
            "BitwiseAnd" => Some(Self::BitwiseAnd),
            "BitwiseOr" => Some(Self::BitwiseOr),
            "BitwiseXor" => Some(Self::BitwiseXor),
            "BitwiseShiftLeft" => Some(Self::BitwiseShiftLeft),
            "BitwiseShiftRight" => Some(Self::BitwiseShiftRight),
            _ => None,
        }
    }
}
