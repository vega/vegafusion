#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskGraphValueError {
    #[prost(string, tag = "1")]
    pub msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Error {
    #[prost(oneof = "error::Errorkind", tags = "1")]
    pub errorkind: ::core::option::Option<error::Errorkind>,
}
/// Nested message and enum types in `Error`.
pub mod error {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Errorkind {
        #[prost(message, tag = "1")]
        Error(super::TaskGraphValueError),
    }
}
