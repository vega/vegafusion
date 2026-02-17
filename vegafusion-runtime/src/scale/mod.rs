#[cfg(feature = "scales")]
pub mod adapter;
pub mod task;
#[cfg(feature = "scales")]
pub mod vega_defaults;
#[cfg(feature = "scales")]
pub mod vega_schemes;

/// Internal sentinel used to represent null-valued categories for server-evaluated
/// discrete scales. Vega allows null as a categorical domain value, but avenger-scales
/// currently rejects null array elements.
pub const DISCRETE_NULL_SENTINEL: &str = "__vf_discrete_null__";
