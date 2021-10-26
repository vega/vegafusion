#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VegaFusionRuntimeRequest {
    #[prost(oneof="vega_fusion_runtime_request::Request", tags="1")]
    pub request: ::core::option::Option<vega_fusion_runtime_request::Request>,
}
/// Nested message and enum types in `VegaFusionRuntimeRequest`.
pub mod vega_fusion_runtime_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag="1")]
        TaskGraphValues(super::super::tasks::TaskGraphValueRequest),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VegaFusionRuntimeResponse {
    #[prost(oneof="vega_fusion_runtime_response::Response", tags="1")]
    pub response: ::core::option::Option<vega_fusion_runtime_response::Response>,
}
/// Nested message and enum types in `VegaFusionRuntimeResponse`.
pub mod vega_fusion_runtime_response {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag="1")]
        TaskGraphValues(super::super::tasks::TaskGraphValueResponse),
    }
}
