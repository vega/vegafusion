//// Pre transform spec messages
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecOpts {
    #[prost(uint32, optional, tag="1")]
    pub row_limit: ::core::option::Option<u32>,
    #[prost(message, repeated, tag="2")]
    pub inline_datasets: ::prost::alloc::vec::Vec<PreTransformInlineDataset>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecRequest {
    #[prost(string, tag="1")]
    pub spec: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub local_tz: ::prost::alloc::string::String,
    #[prost(string, optional, tag="3")]
    pub output_tz: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag="4")]
    pub opts: ::core::option::Option<PreTransformSpecOpts>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecResponse {
    #[prost(string, tag="1")]
    pub spec: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub warnings: ::prost::alloc::vec::Vec<PreTransformSpecWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecWarning {
    #[prost(oneof="pre_transform_spec_warning::WarningType", tags="1, 2, 3, 4")]
    pub warning_type: ::core::option::Option<pre_transform_spec_warning::WarningType>,
}
/// Nested message and enum types in `PreTransformSpecWarning`.
pub mod pre_transform_spec_warning {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WarningType {
        #[prost(message, tag="1")]
        RowLimit(super::PreTransformRowLimitWarning),
        #[prost(message, tag="2")]
        BrokenInteractivity(super::PreTransformBrokenInteractivityWarning),
        #[prost(message, tag="3")]
        Unsupported(super::PreTransformUnsupportedWarning),
        #[prost(message, tag="4")]
        Planner(super::PlannerWarning),
    }
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
//// Pre transform value messages
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformVariable {
    #[prost(message, optional, tag="1")]
    pub variable: ::core::option::Option<super::tasks::Variable>,
    #[prost(uint32, repeated, tag="2")]
    pub scope: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesOpts {
    #[prost(message, repeated, tag="1")]
    pub variables: ::prost::alloc::vec::Vec<PreTransformVariable>,
    #[prost(message, repeated, tag="2")]
    pub inline_datasets: ::prost::alloc::vec::Vec<PreTransformInlineDataset>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesRequest {
    #[prost(string, tag="1")]
    pub spec: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub local_tz: ::prost::alloc::string::String,
    #[prost(string, optional, tag="3")]
    pub default_input_tz: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag="4")]
    pub opts: ::core::option::Option<PreTransformValuesOpts>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesResponse {
    #[prost(message, repeated, tag="1")]
    pub values: ::prost::alloc::vec::Vec<super::tasks::ResponseTaskValue>,
    #[prost(message, repeated, tag="2")]
    pub warnings: ::prost::alloc::vec::Vec<PreTransformValuesWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesWarning {
    #[prost(oneof="pre_transform_values_warning::WarningType", tags="1")]
    pub warning_type: ::core::option::Option<pre_transform_values_warning::WarningType>,
}
/// Nested message and enum types in `PreTransformValuesWarning`.
pub mod pre_transform_values_warning {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WarningType {
        #[prost(message, tag="1")]
        Planner(super::PlannerWarning),
    }
}
//// Common pre-transform messages
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformInlineDataset {
    /// Inline dataset name
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Serialized Arrow record batch in Arrow IPC format
    #[prost(bytes="vec", tag="2")]
    pub table: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PlannerWarning {
    #[prost(string, tag="1")]
    pub message: ::prost::alloc::string::String,
}
