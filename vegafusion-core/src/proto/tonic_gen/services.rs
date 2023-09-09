#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryRequest {
    #[prost(oneof = "query_request::Request", tags = "1")]
    pub request: ::core::option::Option<query_request::Request>,
}
/// Nested message and enum types in `QueryRequest`.
pub mod query_request {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "1")]
        TaskGraphValues(super::super::tasks::TaskGraphValueRequest),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResult {
    #[prost(oneof = "query_result::Response", tags = "1, 2")]
    pub response: ::core::option::Option<query_result::Response>,
}
/// Nested message and enum types in `QueryResult`.
pub mod query_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        Error(super::super::errors::Error),
        #[prost(message, tag = "2")]
        TaskGraphValues(super::super::tasks::TaskGraphValueResponse),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformSpecResult {
    #[prost(oneof = "pre_transform_spec_result::Result", tags = "1, 2")]
    pub result: ::core::option::Option<pre_transform_spec_result::Result>,
}
/// Nested message and enum types in `PreTransformSpecResult`.
pub mod pre_transform_spec_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Error(super::super::errors::Error),
        #[prost(message, tag = "2")]
        Response(super::super::pretransform::PreTransformSpecResponse),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformValuesResult {
    #[prost(oneof = "pre_transform_values_result::Result", tags = "1, 2")]
    pub result: ::core::option::Option<pre_transform_values_result::Result>,
}
/// Nested message and enum types in `PreTransformValuesResult`.
pub mod pre_transform_values_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Error(super::super::errors::Error),
        #[prost(message, tag = "2")]
        Response(super::super::pretransform::PreTransformValuesResponse),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreTransformExtractResult {
    #[prost(oneof = "pre_transform_extract_result::Result", tags = "1, 2")]
    pub result: ::core::option::Option<pre_transform_extract_result::Result>,
}
/// Nested message and enum types in `PreTransformExtractResult`.
pub mod pre_transform_extract_result {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Error(super::super::errors::Error),
        #[prost(message, tag = "2")]
        Response(super::super::pretransform::PreTransformExtractResponse),
    }
}
/// Generated client implementations.
pub mod vega_fusion_runtime_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct VegaFusionRuntimeClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl VegaFusionRuntimeClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> VegaFusionRuntimeClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> VegaFusionRuntimeClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            VegaFusionRuntimeClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn task_graph_query(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRequest>,
        ) -> std::result::Result<tonic::Response<super::QueryResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/services.VegaFusionRuntime/TaskGraphQuery",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("services.VegaFusionRuntime", "TaskGraphQuery"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn pre_transform_spec(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::pretransform::PreTransformSpecRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformSpecResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/services.VegaFusionRuntime/PreTransformSpec",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("services.VegaFusionRuntime", "PreTransformSpec"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn pre_transform_values(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::pretransform::PreTransformValuesRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformValuesResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/services.VegaFusionRuntime/PreTransformValues",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("services.VegaFusionRuntime", "PreTransformValues"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn pre_transform_extract(
            &mut self,
            request: impl tonic::IntoRequest<
                super::super::pretransform::PreTransformExtractRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformExtractResult>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/services.VegaFusionRuntime/PreTransformExtract",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("services.VegaFusionRuntime", "PreTransformExtract"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod vega_fusion_runtime_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with VegaFusionRuntimeServer.
    #[async_trait]
    pub trait VegaFusionRuntime: Send + Sync + 'static {
        async fn task_graph_query(
            &self,
            request: tonic::Request<super::QueryRequest>,
        ) -> std::result::Result<tonic::Response<super::QueryResult>, tonic::Status>;
        async fn pre_transform_spec(
            &self,
            request: tonic::Request<super::super::pretransform::PreTransformSpecRequest>,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformSpecResult>,
            tonic::Status,
        >;
        async fn pre_transform_values(
            &self,
            request: tonic::Request<
                super::super::pretransform::PreTransformValuesRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformValuesResult>,
            tonic::Status,
        >;
        async fn pre_transform_extract(
            &self,
            request: tonic::Request<
                super::super::pretransform::PreTransformExtractRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::PreTransformExtractResult>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct VegaFusionRuntimeServer<T: VegaFusionRuntime> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: VegaFusionRuntime> VegaFusionRuntimeServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for VegaFusionRuntimeServer<T>
    where
        T: VegaFusionRuntime,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/services.VegaFusionRuntime/TaskGraphQuery" => {
                    #[allow(non_camel_case_types)]
                    struct TaskGraphQuerySvc<T: VegaFusionRuntime>(pub Arc<T>);
                    impl<
                        T: VegaFusionRuntime,
                    > tonic::server::UnaryService<super::QueryRequest>
                    for TaskGraphQuerySvc<T> {
                        type Response = super::QueryResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as VegaFusionRuntime>::task_graph_query(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = TaskGraphQuerySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/services.VegaFusionRuntime/PreTransformSpec" => {
                    #[allow(non_camel_case_types)]
                    struct PreTransformSpecSvc<T: VegaFusionRuntime>(pub Arc<T>);
                    impl<
                        T: VegaFusionRuntime,
                    > tonic::server::UnaryService<
                        super::super::pretransform::PreTransformSpecRequest,
                    > for PreTransformSpecSvc<T> {
                        type Response = super::PreTransformSpecResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::pretransform::PreTransformSpecRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as VegaFusionRuntime>::pre_transform_spec(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PreTransformSpecSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/services.VegaFusionRuntime/PreTransformValues" => {
                    #[allow(non_camel_case_types)]
                    struct PreTransformValuesSvc<T: VegaFusionRuntime>(pub Arc<T>);
                    impl<
                        T: VegaFusionRuntime,
                    > tonic::server::UnaryService<
                        super::super::pretransform::PreTransformValuesRequest,
                    > for PreTransformValuesSvc<T> {
                        type Response = super::PreTransformValuesResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::pretransform::PreTransformValuesRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as VegaFusionRuntime>::pre_transform_values(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PreTransformValuesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/services.VegaFusionRuntime/PreTransformExtract" => {
                    #[allow(non_camel_case_types)]
                    struct PreTransformExtractSvc<T: VegaFusionRuntime>(pub Arc<T>);
                    impl<
                        T: VegaFusionRuntime,
                    > tonic::server::UnaryService<
                        super::super::pretransform::PreTransformExtractRequest,
                    > for PreTransformExtractSvc<T> {
                        type Response = super::PreTransformExtractResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::pretransform::PreTransformExtractRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as VegaFusionRuntime>::pre_transform_extract(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PreTransformExtractSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: VegaFusionRuntime> Clone for VegaFusionRuntimeServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: VegaFusionRuntime> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: VegaFusionRuntime> tonic::server::NamedService
    for VegaFusionRuntimeServer<T> {
        const NAME: &'static str = "services.VegaFusionRuntime";
    }
}
