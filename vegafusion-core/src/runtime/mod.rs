mod runtime;
pub use runtime::{PreTransformExtractTable, VegaFusionRuntimeTrait, decode_inline_datasets};

#[cfg(feature = "tonic_support")]
mod grpc_runtime;
#[cfg(feature = "tonic_support")]
pub use grpc_runtime::GrpcVegaFusionRuntime;
