/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
#[cfg(not(feature = "tonic_support"))]
pub mod prost_gen;
#[cfg(not(feature = "tonic_support"))]
pub use prost_gen as gen;

#[cfg(feature = "tonic_support")]
pub mod tonic_gen;
#[cfg(feature = "tonic_support")]
pub use tonic_gen as gen;
