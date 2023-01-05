#[cfg(not(feature = "tonic_support"))]
pub mod prost_gen;
#[cfg(not(feature = "tonic_support"))]
pub use prost_gen as gen;

#[cfg(feature = "tonic_support")]
pub mod tonic_gen;
#[cfg(feature = "tonic_support")]
pub use tonic_gen as gen;
