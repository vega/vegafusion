#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformOpts {
    #[prost(uint32, optional, tag="1")]
    pub row_limit: ::core::option::Option<u32>,
    #[prost(message, repeated, tag="2")]
    pub inline_datasets: ::prost::alloc::vec::Vec<PreTransformInlineDataset>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformRequest {
    #[prost(string, tag="1")]
    pub spec: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub local_tz: ::prost::alloc::string::String,
    #[prost(string, optional, tag="3")]
    pub output_tz: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag="4")]
    pub opts: ::core::option::Option<PreTransformOpts>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformResponse {
    #[prost(string, tag="1")]
    pub spec: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub warnings: ::prost::alloc::vec::Vec<PreTransformWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformRowLimitWarning {
    #[prost(message, repeated, tag="1")]
    pub datasets: ::prost::alloc::vec::Vec<super::tasks::Variable>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformBrokenInteractivityWarning {
    #[prost(message, repeated, tag="1")]
    pub vars: ::prost::alloc::vec::Vec<super::tasks::Variable>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformUnsupportedWarning {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformWarning {
    #[prost(oneof="pre_transform_warning::WarningType", tags="1, 2, 3")]
    pub warning_type: ::core::option::Option<pre_transform_warning::WarningType>,
}
/// Nested message and enum types in `PreTransformWarning`.
pub mod pre_transform_warning {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WarningType {
        #[prost(message, tag="1")]
        RowLimit(super::PreTransformRowLimitWarning),
        #[prost(message, tag="2")]
        BrokenInteractivity(super::PreTransformBrokenInteractivityWarning),
        #[prost(message, tag="3")]
        Unsupported(super::PreTransformUnsupportedWarning),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformInlineDataset {
    /// Inline dataset name
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Serialized Arrow record batch in Arrow IPC format
    #[prost(bytes="vec", tag="2")]
    pub table: ::prost::alloc::vec::Vec<u8>,
}
