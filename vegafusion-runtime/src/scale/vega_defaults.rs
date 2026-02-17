use datafusion_common::ScalarValue;
use std::collections::HashMap;
use vegafusion_common::arrow::array::ArrayRef;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::spec::scale::ScaleTypeSpec;

pub(crate) fn apply_vega_domain_defaults(
    scale_type: &ScaleTypeSpec,
    domain: ArrayRef,
    range: &ArrayRef,
    options: &mut HashMap<String, ScalarValue>,
    domain_from_raw: bool,
) -> Result<ArrayRef> {
    if is_continuous_scale(scale_type) && option_present_non_null(options, "domain_mid") {
        return Err(VegaFusionError::internal(
            "domainMid is not yet supported for server-evaluated piecewise continuous scales",
        ));
    }

    let resolved_domain = if domain_from_raw {
        domain
    } else if is_continuous_scale(scale_type) {
        let (mut d0, mut d1) = domain_bounds_for_scale(scale_type, &domain)?;

        let zero_enabled =
            if let Some(zero_option) = option_scalar(options, "zero").and_then(non_null_scalar) {
                scalar_to_bool(zero_option)?
            } else {
                matches!(
                    scale_type,
                    ScaleTypeSpec::Linear | ScaleTypeSpec::Pow | ScaleTypeSpec::Sqrt
                )
            };

        if zero_enabled {
            if d0 > 0.0 && d1 > 0.0 {
                d0 = 0.0;
            }
            if d0 < 0.0 && d1 < 0.0 {
                d1 = 0.0;
            }
        }

        if let Some(domain_min) = option_scalar(options, "domain_min").and_then(non_null_scalar) {
            d0 = scalar_to_domain_f64(scale_type, domain_min)?;
        }
        if let Some(domain_max) = option_scalar(options, "domain_max").and_then(non_null_scalar) {
            d1 = scalar_to_domain_f64(scale_type, domain_max)?;
        }

        if let Some(padding) = option_scalar(options, "padding").and_then(non_null_scalar) {
            let padding = scalar_to_numeric_f64(padding)?;
            if padding != 0.0 && d0 != d1 {
                let (r0, r1) = range_bounds(range)?;
                let span = (r1 - r0).abs();
                let denom = span - 2.0 * padding;
                if denom != 0.0 {
                    let frac = span / denom;
                    if frac.is_finite() {
                        (d0, d1) = zoom_domain(scale_type, d0, d1, frac, options)?;
                    }
                }
            }
        }

        bounds_to_domain_array(scale_type, d0, d1)?
    } else {
        domain
    };

    consume_construction_options(scale_type, options);
    Ok(resolved_domain)
}

pub(crate) fn consume_construction_options(
    scale_type: &ScaleTypeSpec,
    options: &mut HashMap<String, ScalarValue>,
) {
    for key in [
        "domain_raw",
        "domain_min",
        "domain_max",
        "domain_mid",
        "range_step",
    ] {
        options.remove(key);
    }

    if is_continuous_scale(scale_type) {
        options.remove("padding");
        options.remove("zero");
    }
}

fn is_continuous_scale(scale_type: &ScaleTypeSpec) -> bool {
    matches!(
        scale_type,
        ScaleTypeSpec::Linear
            | ScaleTypeSpec::Log
            | ScaleTypeSpec::Pow
            | ScaleTypeSpec::Sqrt
            | ScaleTypeSpec::Symlog
            | ScaleTypeSpec::Time
            | ScaleTypeSpec::Utc
    )
}

fn option_scalar<'a>(
    options: &'a HashMap<String, ScalarValue>,
    key: &str,
) -> Option<&'a ScalarValue> {
    options.get(key)
}

fn non_null_scalar(scalar: &ScalarValue) -> Option<&ScalarValue> {
    if scalar.is_null() {
        None
    } else {
        Some(scalar)
    }
}

fn option_present_non_null(options: &HashMap<String, ScalarValue>, key: &str) -> bool {
    option_scalar(options, key)
        .and_then(non_null_scalar)
        .is_some()
}

fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        _ => Err(VegaFusionError::internal(format!(
            "Expected boolean scalar value, received {value:?}"
        ))),
    }
}

fn scalar_to_numeric_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Float32(Some(v)) => Ok(*v as f64),
        ScalarValue::Boolean(Some(v)) => Ok(if *v { 1.0 } else { 0.0 }),
        ScalarValue::Int8(Some(v)) => Ok(*v as f64),
        ScalarValue::Int16(Some(v)) => Ok(*v as f64),
        ScalarValue::Int32(Some(v)) => Ok(*v as f64),
        ScalarValue::Int64(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt8(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as f64),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => v.parse::<f64>().map_err(|err| {
            VegaFusionError::internal(format!(
                "Expected numeric scalar value, received {value:?} (failed to parse string: {err})"
            ))
        }),
        _ => Err(VegaFusionError::internal(format!(
            "Expected numeric scalar value, received {value:?}"
        ))),
    }
}

fn scalar_to_millis(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Date32(Some(days)) => Ok((*days as f64) * 86_400_000.0),
        ScalarValue::Date64(Some(ms)) => Ok(*ms as f64),
        ScalarValue::TimestampSecond(Some(v), _) => Ok((*v as f64) * 1000.0),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v as f64),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Ok((*v as f64) / 1000.0),
        ScalarValue::TimestampNanosecond(Some(v), _) => Ok((*v as f64) / 1_000_000.0),
        _ => scalar_to_numeric_f64(value),
    }
}

fn scalar_to_domain_f64(scale_type: &ScaleTypeSpec, value: &ScalarValue) -> Result<f64> {
    if matches!(scale_type, ScaleTypeSpec::Time | ScaleTypeSpec::Utc) {
        scalar_to_millis(value)
    } else {
        scalar_to_numeric_f64(value)
    }
}

fn domain_bounds_for_scale(scale_type: &ScaleTypeSpec, domain: &ArrayRef) -> Result<(f64, f64)> {
    if domain.len() < 2 {
        return Err(VegaFusionError::internal(format!(
            "Continuous scale domain must include at least two values, received {}",
            domain.len()
        )));
    }

    let lo = ScalarValue::try_from_array(domain, 0)?;
    let hi = ScalarValue::try_from_array(domain, domain.len() - 1)?;
    Ok((
        scalar_to_domain_f64(scale_type, &lo)?,
        scalar_to_domain_f64(scale_type, &hi)?,
    ))
}

fn range_bounds(range: &ArrayRef) -> Result<(f64, f64)> {
    if range.len() < 2 {
        return Err(VegaFusionError::internal(format!(
            "Scale range must include at least two values, received {}",
            range.len()
        )));
    }
    let lo = ScalarValue::try_from_array(range, 0)?;
    let hi = ScalarValue::try_from_array(range, range.len() - 1)?;
    Ok((scalar_to_numeric_f64(&lo)?, scalar_to_numeric_f64(&hi)?))
}

fn bounds_to_domain_array(scale_type: &ScaleTypeSpec, d0: f64, d1: f64) -> Result<ArrayRef> {
    if matches!(scale_type, ScaleTypeSpec::Time | ScaleTypeSpec::Utc) {
        Ok(ScalarValue::iter_to_array(vec![
            ScalarValue::TimestampMillisecond(Some(d0.round() as i64), None),
            ScalarValue::TimestampMillisecond(Some(d1.round() as i64), None),
        ])?)
    } else {
        Ok(ScalarValue::iter_to_array(vec![
            ScalarValue::from(d0),
            ScalarValue::from(d1),
        ])?)
    }
}

fn zoom_domain(
    scale_type: &ScaleTypeSpec,
    d0: f64,
    d1: f64,
    scale: f64,
    options: &HashMap<String, ScalarValue>,
) -> Result<(f64, f64)> {
    let da = (d0 + d1) / 2.0;

    match scale_type {
        ScaleTypeSpec::Linear | ScaleTypeSpec::Time | ScaleTypeSpec::Utc => {
            Ok((da + (d0 - da) * scale, da + (d1 - da) * scale))
        }
        ScaleTypeSpec::Log => {
            let sign = if d0 < 0.0 {
                -1.0
            } else if d0 > 0.0 {
                1.0
            } else {
                return Ok((d0, d1));
            };
            let lift = |x: f64| (sign * x).ln();
            let ground = |x: f64| sign * x.exp();
            let l0 = lift(d0);
            let l1 = lift(d1);
            let la = (l0 + l1) / 2.0;
            Ok((
                ground(la + (l0 - la) * scale),
                ground(la + (l1 - la) * scale),
            ))
        }
        ScaleTypeSpec::Pow | ScaleTypeSpec::Sqrt => {
            let exponent = if matches!(scale_type, ScaleTypeSpec::Sqrt) {
                0.5
            } else {
                option_scalar(options, "exponent")
                    .and_then(non_null_scalar)
                    .map(scalar_to_numeric_f64)
                    .transpose()?
                    .unwrap_or(1.0)
            };
            if exponent == 0.0 {
                return Err(VegaFusionError::internal(
                    "Scale exponent must be non-zero for pow/sqrt padding",
                ));
            }
            let lift = |x: f64| {
                if x < 0.0 {
                    -(-x).powf(exponent)
                } else {
                    x.powf(exponent)
                }
            };
            let inv_exponent = 1.0 / exponent;
            let ground = |x: f64| {
                if x < 0.0 {
                    -(-x).powf(inv_exponent)
                } else {
                    x.powf(inv_exponent)
                }
            };
            let l0 = lift(d0);
            let l1 = lift(d1);
            let la = (l0 + l1) / 2.0;
            Ok((
                ground(la + (l0 - la) * scale),
                ground(la + (l1 - la) * scale),
            ))
        }
        ScaleTypeSpec::Symlog => {
            let constant = option_scalar(options, "constant")
                .and_then(non_null_scalar)
                .map(scalar_to_numeric_f64)
                .transpose()?
                .unwrap_or(1.0);
            let lift = |x: f64| x.signum() * (x / constant).abs().ln_1p();
            let ground = |x: f64| x.signum() * x.abs().exp_m1() * constant;
            let l0 = lift(d0);
            let l1 = lift(d1);
            let la = (l0 + l1) / 2.0;
            Ok((
                ground(la + (l0 - la) * scale),
                ground(la + (l1 - la) * scale),
            ))
        }
        _ => Ok((d0, d1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vegafusion_common::arrow::array::Float64Array;

    fn f64_domain(values: &[f64]) -> ArrayRef {
        std::sync::Arc::new(Float64Array::from(values.to_vec()))
    }

    #[test]
    fn test_linear_default_zero_applies() {
        let mut options = HashMap::new();
        let domain = f64_domain(&[2.0, 5.0]);
        let range = f64_domain(&[100.0, 0.0]);
        let resolved =
            apply_vega_domain_defaults(&ScaleTypeSpec::Linear, domain, &range, &mut options, false)
                .unwrap();
        let lo = ScalarValue::try_from_array(&resolved, 0).unwrap();
        let hi = ScalarValue::try_from_array(&resolved, 1).unwrap();
        assert_eq!(scalar_to_numeric_f64(&lo).unwrap(), 0.0);
        assert_eq!(scalar_to_numeric_f64(&hi).unwrap(), 5.0);
    }

    #[test]
    fn test_linear_explicit_zero_false() {
        let mut options = HashMap::from([("zero".to_string(), ScalarValue::from(false))]);
        let domain = f64_domain(&[2.0, 5.0]);
        let range = f64_domain(&[100.0, 0.0]);
        let resolved =
            apply_vega_domain_defaults(&ScaleTypeSpec::Linear, domain, &range, &mut options, false)
                .unwrap();
        let lo = ScalarValue::try_from_array(&resolved, 0).unwrap();
        let hi = ScalarValue::try_from_array(&resolved, 1).unwrap();
        assert_eq!(scalar_to_numeric_f64(&lo).unwrap(), 2.0);
        assert_eq!(scalar_to_numeric_f64(&hi).unwrap(), 5.0);
    }

    #[test]
    fn test_domain_raw_skips_adjustments() {
        let mut options = HashMap::from([
            ("zero".to_string(), ScalarValue::from(true)),
            ("padding".to_string(), ScalarValue::from(10.0)),
            ("domain_raw".to_string(), ScalarValue::from(1.0)),
        ]);
        let domain = f64_domain(&[2.0, 5.0]);
        let range = f64_domain(&[100.0, 0.0]);
        let resolved =
            apply_vega_domain_defaults(&ScaleTypeSpec::Linear, domain, &range, &mut options, true)
                .unwrap();
        let lo = ScalarValue::try_from_array(&resolved, 0).unwrap();
        let hi = ScalarValue::try_from_array(&resolved, 1).unwrap();
        assert_eq!(scalar_to_numeric_f64(&lo).unwrap(), 2.0);
        assert_eq!(scalar_to_numeric_f64(&hi).unwrap(), 5.0);
    }

    #[test]
    fn test_domain_mid_errors() {
        let mut options = HashMap::from([("domain_mid".to_string(), ScalarValue::from(3.0))]);
        let domain = f64_domain(&[2.0, 5.0]);
        let range = f64_domain(&[100.0, 0.0]);
        let err =
            apply_vega_domain_defaults(&ScaleTypeSpec::Linear, domain, &range, &mut options, false)
                .unwrap_err();
        assert!(err.to_string().contains(
            "domainMid is not yet supported for server-evaluated piecewise continuous scales"
        ));
    }
}
