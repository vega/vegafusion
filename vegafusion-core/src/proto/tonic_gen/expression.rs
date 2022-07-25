/// ESTree-style AST nodes
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    #[prost(int32, tag="1")]
    pub start: i32,
    #[prost(int32, tag="2")]
    pub end: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Literal {
    #[prost(string, tag="1")]
    pub raw: ::prost::alloc::string::String,
    #[prost(oneof="literal::Value", tags="2, 3, 4, 5")]
    pub value: ::core::option::Option<literal::Value>,
}
/// Nested message and enum types in `Literal`.
pub mod literal {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag="2")]
        String(::prost::alloc::string::String),
        #[prost(bool, tag="3")]
        Boolean(bool),
        #[prost(double, tag="4")]
        Number(f64),
        #[prost(bool, tag="5")]
        Null(bool),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IdentifierAbc {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Identifier {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnaryExpression {
    #[prost(enumeration="UnaryOperator", tag="1")]
    pub operator: i32,
    #[prost(bool, tag="2")]
    pub prefix: bool,
    #[prost(message, optional, boxed, tag="3")]
    pub argument: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExpression {
    #[prost(message, optional, boxed, tag="1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(enumeration="LogicalOperator", tag="2")]
    pub operator: i32,
    #[prost(message, optional, boxed, tag="3")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryExpression {
    #[prost(message, optional, boxed, tag="1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(enumeration="BinaryOperator", tag="2")]
    pub operator: i32,
    #[prost(message, optional, boxed, tag="3")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConditionalExpression {
    #[prost(message, optional, boxed, tag="1")]
    pub test: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag="2")]
    pub consequent: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag="3")]
    pub alternate: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MemberExpression {
    #[prost(message, optional, boxed, tag="1")]
    pub object: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(message, optional, boxed, tag="2")]
    pub property: ::core::option::Option<::prost::alloc::boxed::Box<Expression>>,
    #[prost(bool, tag="3")]
    pub computed: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayExpression {
    #[prost(message, repeated, tag="1")]
    pub elements: ::prost::alloc::vec::Vec<Expression>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CallExpression {
    #[prost(string, tag="1")]
    pub callee: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub arguments: ::prost::alloc::vec::Vec<Expression>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Property {
    #[prost(message, optional, tag="3")]
    pub value: ::core::option::Option<Expression>,
    #[prost(string, tag="4")]
    pub kind: ::prost::alloc::string::String,
    #[prost(oneof="property::Key", tags="1, 2")]
    pub key: ::core::option::Option<property::Key>,
}
/// Nested message and enum types in `Property`.
pub mod property {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Key {
        #[prost(message, tag="1")]
        Literal(super::Literal),
        #[prost(message, tag="2")]
        Identifier(super::Identifier),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectExpression {
    #[prost(message, repeated, tag="1")]
    pub properties: ::prost::alloc::vec::Vec<Property>,
}
/// Top-level expression
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expression {
    #[prost(message, optional, tag="11")]
    pub span: ::core::option::Option<Span>,
    #[prost(oneof="expression::Expr", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub expr: ::core::option::Option<expression::Expr>,
}
/// Nested message and enum types in `Expression`.
pub mod expression {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Expr {
        #[prost(message, tag="1")]
        Identifier(super::Identifier),
        #[prost(message, tag="2")]
        Literal(super::Literal),
        #[prost(message, tag="3")]
        Binary(::prost::alloc::boxed::Box<super::BinaryExpression>),
        #[prost(message, tag="4")]
        Logical(::prost::alloc::boxed::Box<super::LogicalExpression>),
        #[prost(message, tag="5")]
        Unary(::prost::alloc::boxed::Box<super::UnaryExpression>),
        #[prost(message, tag="6")]
        Conditional(::prost::alloc::boxed::Box<super::ConditionalExpression>),
        #[prost(message, tag="7")]
        Call(super::CallExpression),
        #[prost(message, tag="8")]
        Array(super::ArrayExpression),
        #[prost(message, tag="9")]
        Object(super::ObjectExpression),
        #[prost(message, tag="10")]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LogicalOperator {
    Or = 0,
    And = 1,
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
}
