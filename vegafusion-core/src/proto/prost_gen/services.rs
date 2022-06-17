#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryRequest {
    #[prost(oneof="query_request::Request", tags="1")]
    pub request: ::core::option::Option<query_request::Request>,
}
/// Nested message and enum types in `QueryRequest`.
pub mod query_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag="1")]
        TaskGraphValues(super::super::tasks::TaskGraphValueRequest),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResult {
    #[prost(oneof="query_result::Response", tags="1, 2")]
    pub response: ::core::option::Option<query_result::Response>,
}
/// Nested message and enum types in `QueryResult`.
pub mod query_result {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag="1")]
        Error(super::super::errors::Error),
        #[prost(message, tag="2")]
        TaskGraphValues(super::super::tasks::TaskGraphValueResponse),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecResult {
    #[prost(oneof="pre_transform_spec_result::Result", tags="1, 2")]
    pub result: ::core::option::Option<pre_transform_spec_result::Result>,
}
/// Nested message and enum types in `PreTransformSpecResult`.
pub mod pre_transform_spec_result {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag="1")]
        Error(super::super::errors::Error),
        #[prost(message, tag="2")]
        Response(super::super::pretransform::PreTransformSpecResponse),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesResult {
    #[prost(oneof="pre_transform_values_result::Result", tags="1, 2")]
    pub result: ::core::option::Option<pre_transform_values_result::Result>,
}
/// Nested message and enum types in `PreTransformValuesResult`.
pub mod pre_transform_values_result {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag="1")]
        Error(super::super::errors::Error),
        #[prost(message, tag="2")]
        Response(super::super::pretransform::PreTransformValuesResponse),
    }
}
