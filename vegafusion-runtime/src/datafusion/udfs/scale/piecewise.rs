use avenger_scales::scales::linear::LinearScale;
use avenger_scales::scales::ConfiguredScale;
use std::sync::Arc;
use vegafusion_common::arrow::array::{new_empty_array, new_null_array, ArrayRef, Float32Array};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use vegafusion_core::error::Result;
use vegafusion_core::spec::scale::ScaleTypeSpec;
use vegafusion_core::task_graph::scale_state::ScaleState;

use super::{looks_like_color_output_type, scalar_to_f64, scalar_to_numeric_or_temporal_f64};

// Temporary runtime shim for Vega-style domainMid/piecewise continuous scales.
//
// Desired future avenger-scales support:
// - First-class piecewise continuous scales with N-stop domain/range support
// - Piecewise forward mapping and numeric invert behavior
// - Discretizing invert extent helpers for quantize/threshold
//
// When avenger-scales provides these primitives, VegaFusion can simplify by removing
// this module and routing all scale/invert paths through ConfiguredScale.
#[derive(Debug, Clone)]
pub(super) struct PiecewiseScale {
    pub(super) scale_type: ScaleTypeSpec,
    pub(super) d0: f64,
    pub(super) d1: f64,
    pub(super) d2: f64,
    exponent: f64,
    constant: f64,
    domain_dtype: DataType,
    domain_array: ArrayRef,
    pub(super) range_scale: Arc<ConfiguredScale>,
    pub(super) color_output: bool,
}

pub(super) fn try_make_piecewise_scale(scale_state: &ScaleState) -> Result<Option<PiecewiseScale>> {
    if !is_piecewise_candidate(scale_state) {
        return Ok(None);
    }

    let d0 = ScalarValue::try_from_array(scale_state.domain.as_ref(), 0)?;
    let d1 = ScalarValue::try_from_array(scale_state.domain.as_ref(), 1)?;
    let d2 = ScalarValue::try_from_array(scale_state.domain.as_ref(), 2)?;
    let d0 = scalar_to_numeric_or_temporal_f64(&d0).ok_or_else(|| {
        vegafusion_common::error::VegaFusionError::internal(
            "Failed to parse piecewise domain start as numeric",
        )
    })?;
    let d1 = scalar_to_numeric_or_temporal_f64(&d1).ok_or_else(|| {
        vegafusion_common::error::VegaFusionError::internal(
            "Failed to parse piecewise domain midpoint as numeric",
        )
    })?;
    let d2 = scalar_to_numeric_or_temporal_f64(&d2).ok_or_else(|| {
        vegafusion_common::error::VegaFusionError::internal(
            "Failed to parse piecewise domain end as numeric",
        )
    })?;

    let exponent = match scale_state.scale_type {
        ScaleTypeSpec::Sqrt => 0.5,
        ScaleTypeSpec::Pow => option_numeric_f64(&scale_state.options, "exponent", 1.0)?,
        _ => 1.0,
    };
    let constant = option_numeric_f64(&scale_state.options, "constant", 1.0)?;

    let range_scale = Arc::new(
        LinearScale::configured((0.0, 1.0), (0.0, 1.0)).with_range(scale_state.range.clone()),
    );
    let sample = Arc::new(Float32Array::from(vec![0.5_f32])) as ArrayRef;
    let sample_output = range_scale.scale(&sample).map_err(|err| {
        vegafusion_common::error::VegaFusionError::internal(format!(
            "Failed to construct piecewise range scale for diverging domainMid: {err}"
        ))
    })?;
    let color_output = looks_like_color_output_type(sample_output.data_type());

    let mut piecewise = PiecewiseScale {
        scale_type: scale_state.scale_type.clone(),
        d0,
        d1,
        d2,
        exponent,
        constant,
        domain_dtype: scale_state.domain.data_type().clone(),
        domain_array: scale_state.domain.clone(),
        range_scale,
        color_output,
    };
    piecewise.validate_lifted_domain()?;
    Ok(Some(piecewise))
}

pub(super) fn apply_piecewise_scale(
    scale_name: &str,
    values: &ArrayRef,
    piecewise: &PiecewiseScale,
) -> DFResult<ArrayRef> {
    let t_values = (0..values.len())
        .map(|row| {
            if values.is_null(row) {
                return Ok(ScalarValue::Float32(None));
            }
            let scalar = ScalarValue::try_from_array(values.as_ref(), row)?;
            let Some(value) = scalar_to_numeric_or_temporal_f64(&scalar) else {
                return Ok(ScalarValue::Float32(None));
            };
            let Some(t) = piecewise.forward_t(value) else {
                return Ok(ScalarValue::Float32(None));
            };
            if t.is_finite() {
                Ok(ScalarValue::Float32(Some(t as f32)))
            } else {
                Ok(ScalarValue::Float32(None))
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    let t_array = ScalarValue::iter_to_array(t_values)?;
    piecewise.range_scale.scale(&t_array).map_err(|err| {
        DataFusionError::Execution(format!(
            "Failed evaluating diverging scale('{scale_name}', ...): {err}"
        ))
    })
}

pub(super) fn apply_piecewise_invert(
    scale_name: &str,
    values: &ArrayRef,
    piecewise: &PiecewiseScale,
) -> DFResult<ArrayRef> {
    if piecewise.color_output {
        return Ok(new_null_array(piecewise.domain_dtype(), values.len()));
    }

    let t_values = piecewise.range_scale.invert(values).map_err(|err| {
        DataFusionError::Execution(format!(
            "Failed evaluating diverging invert('{scale_name}', ...): {err}"
        ))
    })?;
    let inverted = (0..t_values.len())
        .map(|row| {
            if t_values.is_null(row) {
                return Ok(ScalarValue::Null.cast_to(piecewise.domain_dtype())?);
            }
            let scalar = ScalarValue::try_from_array(t_values.as_ref(), row)?;
            let Some(t) = scalar_to_numeric_or_temporal_f64(&scalar) else {
                return Ok(ScalarValue::Null.cast_to(piecewise.domain_dtype())?);
            };
            let Some(v) = piecewise.inverse_t(t) else {
                return Ok(ScalarValue::Null.cast_to(piecewise.domain_dtype())?);
            };
            let value = ScalarValue::Float64(Some(v)).cast_to(piecewise.domain_dtype())?;
            Ok(value)
        })
        .collect::<DFResult<Vec<_>>>()?;
    ScalarValue::iter_to_array(inverted)
}

pub(super) fn apply_discretizing_invert(
    scale_type: &ScaleTypeSpec,
    configured: &ConfiguredScale,
    values: &ArrayRef,
) -> DFResult<ArrayRef> {
    let extents = (0..values.len())
        .map(|row| {
            if values.is_null(row) {
                return Ok(ScalarValue::Null);
            }
            let range_value = ScalarValue::try_from_array(values.as_ref(), row)?;
            let extent = extent_for_range_value(scale_type, configured, &range_value)?;
            Ok(extent_to_list_scalar(extent))
        })
        .collect::<DFResult<Vec<_>>>()?;
    ScalarValue::iter_to_array(extents)
}

pub(super) fn apply_discretizing_invert_interval(
    scale_type: &ScaleTypeSpec,
    configured: &ConfiguredScale,
    values: &ArrayRef,
) -> DFResult<ArrayRef> {
    if values.len() < 2 || values.is_null(0) || values.is_null(1) {
        return Ok(new_null_array(&DataType::Float64, 2));
    }
    let lo = ScalarValue::try_from_array(values.as_ref(), 0)?;
    let hi = ScalarValue::try_from_array(values.as_ref(), 1)?;
    let Some((min_index, max_index)) = range_interval_indices(configured, &lo, &hi)? else {
        return Ok(new_null_array(&DataType::Float64, 2));
    };

    let lo_extent = extent_for_range_index(scale_type, configured, min_index)?.normalize_for_low();
    let hi_extent = extent_for_range_index(scale_type, configured, max_index)?.normalize_for_high();
    let lo_scalar = lo_extent
        .map(ScalarValue::from)
        .unwrap_or(ScalarValue::Float64(None));
    let hi_scalar = hi_extent
        .map(ScalarValue::from)
        .unwrap_or(ScalarValue::Float64(None));
    ScalarValue::iter_to_array(vec![lo_scalar, hi_scalar])
}

impl PiecewiseScale {
    pub(super) fn domain_dtype(&self) -> &DataType {
        &self.domain_dtype
    }

    pub(super) fn domain_array(&self) -> &ArrayRef {
        &self.domain_array
    }

    fn validate_lifted_domain(&mut self) -> Result<()> {
        for (label, value) in [("start", self.d0), ("mid", self.d1), ("end", self.d2)] {
            if self.lift(value).is_none() {
                return Err(vegafusion_common::error::VegaFusionError::internal(
                    format!(
                        "Invalid piecewise domain {label} value {value} for {:?} scale",
                        self.scale_type
                    ),
                ));
            }
        }
        Ok(())
    }

    fn lift(&self, value: f64) -> Option<f64> {
        match self.scale_type {
            ScaleTypeSpec::Linear
            | ScaleTypeSpec::Sequential
            | ScaleTypeSpec::Time
            | ScaleTypeSpec::Utc => Some(value),
            ScaleTypeSpec::Log => {
                let sign = if self.d0 < 0.0 { -1.0 } else { 1.0 };
                let signed = sign * value;
                if signed <= 0.0 {
                    None
                } else {
                    Some(signed.ln())
                }
            }
            ScaleTypeSpec::Pow | ScaleTypeSpec::Sqrt => {
                if self.exponent == 0.0 {
                    None
                } else if value < 0.0 {
                    Some(-(-value).powf(self.exponent))
                } else {
                    Some(value.powf(self.exponent))
                }
            }
            ScaleTypeSpec::Symlog => {
                let c = self.constant;
                if c == 0.0 {
                    None
                } else {
                    Some(value.signum() * (value / c).abs().ln_1p())
                }
            }
            _ => None,
        }
    }

    fn ground(&self, value: f64) -> Option<f64> {
        match self.scale_type {
            ScaleTypeSpec::Linear
            | ScaleTypeSpec::Sequential
            | ScaleTypeSpec::Time
            | ScaleTypeSpec::Utc => Some(value),
            ScaleTypeSpec::Log => {
                let sign = if self.d0 < 0.0 { -1.0 } else { 1.0 };
                Some(sign * value.exp())
            }
            ScaleTypeSpec::Pow | ScaleTypeSpec::Sqrt => {
                if self.exponent == 0.0 {
                    None
                } else {
                    let inv = 1.0 / self.exponent;
                    if value < 0.0 {
                        Some(-(-value).powf(inv))
                    } else {
                        Some(value.powf(inv))
                    }
                }
            }
            ScaleTypeSpec::Symlog => {
                let c = self.constant;
                if c == 0.0 {
                    None
                } else {
                    Some(value.signum() * value.abs().exp_m1() * c)
                }
            }
            _ => None,
        }
    }

    fn forward_t(&self, value: f64) -> Option<f64> {
        let lv = self.lift(value)?;
        let l0 = self.lift(self.d0)?;
        let l1 = self.lift(self.d1)?;
        let l2 = self.lift(self.d2)?;

        let increasing = l2 >= l0;
        let use_left = if increasing { lv <= l1 } else { lv >= l1 };
        if use_left {
            let denom = l1 - l0;
            if denom == 0.0 {
                Some(0.5)
            } else {
                Some(0.5 * (lv - l0) / denom)
            }
        } else {
            let denom = l2 - l1;
            if denom == 0.0 {
                Some(0.5)
            } else {
                Some(0.5 + 0.5 * (lv - l1) / denom)
            }
        }
    }

    fn inverse_t(&self, t: f64) -> Option<f64> {
        let l0 = self.lift(self.d0)?;
        let l1 = self.lift(self.d1)?;
        let l2 = self.lift(self.d2)?;
        let lv = if t <= 0.5 {
            l0 + (t / 0.5) * (l1 - l0)
        } else {
            l1 + ((t - 0.5) / 0.5) * (l2 - l1)
        };
        self.ground(lv)
    }
}

#[derive(Debug, Clone, Copy)]
struct DomainExtent {
    lo: Option<f64>,
    hi: Option<f64>,
}

impl DomainExtent {
    fn normalize_for_low(self) -> Option<f64> {
        self.lo.or(self.hi)
    }

    fn normalize_for_high(self) -> Option<f64> {
        self.hi.or(self.lo)
    }
}

fn is_piecewise_candidate(scale_state: &ScaleState) -> bool {
    scale_state.domain.len() == 3
        && matches!(
            scale_state.scale_type,
            ScaleTypeSpec::Linear
                | ScaleTypeSpec::Sequential
                | ScaleTypeSpec::Log
                | ScaleTypeSpec::Pow
                | ScaleTypeSpec::Sqrt
                | ScaleTypeSpec::Symlog
                | ScaleTypeSpec::Time
                | ScaleTypeSpec::Utc
        )
}

fn option_numeric_f64(
    options: &std::collections::HashMap<String, ScalarValue>,
    key: &str,
    default: f64,
) -> Result<f64> {
    match options.get(key) {
        Some(value) if !value.is_null() => match value {
            ScalarValue::Float64(Some(v)) => Ok(*v),
            ScalarValue::Float32(Some(v)) => Ok(*v as f64),
            ScalarValue::Int8(Some(v)) => Ok(*v as f64),
            ScalarValue::Int16(Some(v)) => Ok(*v as f64),
            ScalarValue::Int32(Some(v)) => Ok(*v as f64),
            ScalarValue::Int64(Some(v)) => Ok(*v as f64),
            ScalarValue::UInt8(Some(v)) => Ok(*v as f64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as f64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as f64),
            ScalarValue::UInt64(Some(v)) => Ok(*v as f64),
            _ => Err(vegafusion_common::error::VegaFusionError::internal(
                format!("Expected numeric option {key:?}, received {value:?}"),
            )),
        },
        _ => Ok(default),
    }
}

fn extent_to_list_scalar(extent: DomainExtent) -> ScalarValue {
    let lo = extent
        .lo
        .map(ScalarValue::from)
        .unwrap_or(ScalarValue::Float64(None));
    let hi = extent
        .hi
        .map(ScalarValue::from)
        .unwrap_or(ScalarValue::Float64(None));
    let values = ScalarValue::iter_to_array(vec![lo, hi])
        .unwrap_or_else(|_| new_empty_array(&DataType::Float64));
    ScalarValue::List(Arc::new(
        datafusion_common::utils::SingleRowListArrayBuilder::new(values)
            .with_nullable(true)
            .build_list_array(),
    ))
}

fn extent_for_range_value(
    scale_type: &ScaleTypeSpec,
    configured: &ConfiguredScale,
    range_value: &ScalarValue,
) -> DFResult<DomainExtent> {
    let Some(index) = range_index(configured, range_value)? else {
        return Ok(DomainExtent {
            lo: Some(f64::NAN),
            hi: Some(f64::NAN),
        });
    };
    extent_for_range_index(scale_type, configured, index)
}

fn extent_for_range_index(
    scale_type: &ScaleTypeSpec,
    configured: &ConfiguredScale,
    index: usize,
) -> DFResult<DomainExtent> {
    match scale_type {
        ScaleTypeSpec::Quantize => quantize_extent_for_index(configured, index),
        ScaleTypeSpec::Threshold => threshold_extent_for_index(configured, index),
        _ => Err(DataFusionError::Execution(format!(
            "Discretizing invert is only supported for quantize/threshold, received {:?}",
            scale_type
        ))),
    }
}

fn quantize_extent_for_index(configured: &ConfiguredScale, index: usize) -> DFResult<DomainExtent> {
    let normalized_domain = configured.normalized_domain().map_err(|err| {
        DataFusionError::Execution(format!(
            "Failed to obtain normalized quantize domain for invertExtent: {err}"
        ))
    })?;
    if normalized_domain.len() < 2 {
        return Err(DataFusionError::Execution(
            "Quantize scale domain must have at least two values".to_string(),
        ));
    }
    let d0 = ScalarValue::try_from_array(normalized_domain.as_ref(), 0)?;
    let d1 = ScalarValue::try_from_array(normalized_domain.as_ref(), normalized_domain.len() - 1)?;
    let d0 = scalar_to_f64(&d0)?;
    let d1 = scalar_to_f64(&d1)?;
    let n = configured.range().len();
    if n == 0 {
        return Err(DataFusionError::Execution(
            "Quantize scale range must have at least one value".to_string(),
        ));
    }
    let step = (d1 - d0) / (n as f64);
    let lo = d0 + (index as f64) * step;
    let hi = d0 + ((index + 1) as f64) * step;
    Ok(DomainExtent {
        lo: Some(lo),
        hi: Some(hi),
    })
}

fn threshold_extent_for_index(
    configured: &ConfiguredScale,
    index: usize,
) -> DFResult<DomainExtent> {
    let domain = configured.domain();
    let threshold_len = domain.len();
    let lo = if index == 0 {
        None
    } else {
        Some(scalar_to_f64(&ScalarValue::try_from_array(
            domain,
            index - 1,
        )?)?)
    };
    let hi = if index >= threshold_len {
        None
    } else {
        Some(scalar_to_f64(&ScalarValue::try_from_array(domain, index)?)?)
    };
    Ok(DomainExtent { lo, hi })
}

fn range_index(configured: &ConfiguredScale, value: &ScalarValue) -> DFResult<Option<usize>> {
    let range = configured.range();
    for idx in 0..range.len() {
        let range_value = ScalarValue::try_from_array(range, idx)?;
        if scalar_eq_for_range_lookup(&range_value, value)? {
            return Ok(Some(idx));
        }
    }
    Ok(None)
}

fn scalar_eq_for_range_lookup(left: &ScalarValue, right: &ScalarValue) -> DFResult<bool> {
    if left == right {
        return Ok(true);
    }
    if left.is_null() || right.is_null() {
        return Ok(false);
    }

    if let (Ok(l), Ok(r)) = (scalar_to_f64(left), scalar_to_f64(right)) {
        return Ok(l.total_cmp(&r) == std::cmp::Ordering::Equal);
    }

    Ok(left.to_string() == right.to_string())
}

fn range_interval_indices(
    configured: &ConfiguredScale,
    lo_value: &ScalarValue,
    hi_value: &ScalarValue,
) -> DFResult<Option<(usize, usize)>> {
    let range = configured.range();
    let (lo, hi) = if scalar_cmp_for_range_lookup(lo_value, hi_value)?.is_gt() {
        (hi_value, lo_value)
    } else {
        (lo_value, hi_value)
    };

    let mut min_index: Option<usize> = None;
    let mut max_index: Option<usize> = None;
    for idx in 0..range.len() {
        let value = ScalarValue::try_from_array(range, idx)?;
        let ge_lo = scalar_cmp_for_range_lookup(&value, lo)?.is_ge();
        let le_hi = scalar_cmp_for_range_lookup(&value, hi)?.is_le();
        if ge_lo && le_hi {
            min_index.get_or_insert(idx);
            max_index = Some(idx);
        }
    }

    match (min_index, max_index) {
        (Some(min_index), Some(max_index)) => Ok(Some((min_index, max_index))),
        _ => Ok(None),
    }
}

fn scalar_cmp_for_range_lookup(
    left: &ScalarValue,
    right: &ScalarValue,
) -> DFResult<std::cmp::Ordering> {
    if left.is_null() && right.is_null() {
        return Ok(std::cmp::Ordering::Equal);
    }
    if left.is_null() {
        return Ok(std::cmp::Ordering::Less);
    }
    if right.is_null() {
        return Ok(std::cmp::Ordering::Greater);
    }
    if let (Ok(l), Ok(r)) = (scalar_to_f64(left), scalar_to_f64(right)) {
        return Ok(l.total_cmp(&r));
    }
    Ok(left.to_string().cmp(&right.to_string()))
}
