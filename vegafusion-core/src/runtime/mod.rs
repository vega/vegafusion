mod runtime;
pub use runtime::{
    decode_inline_datasets, encode_inline_datasets, PreTransformExtractTable,
    VegaFusionRuntimeTrait,
};

#[cfg(feature = "tonic_support")]
mod grpc_runtime;
#[cfg(feature = "tonic_support")]
pub use grpc_runtime::GrpcVegaFusionRuntime;
