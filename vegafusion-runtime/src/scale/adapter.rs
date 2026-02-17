use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_common::ScalarValue;
use std::collections::HashMap;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::spec::scale::ScaleTypeSpec;
use vegafusion_core::task_graph::scale_state::ScaleState;

use avenger_scales::scales::{
    band::BandScale, linear::LinearScale, log::LogScale, ordinal::OrdinalScale, point::PointScale,
    pow::PowScale, symlog::SymlogScale, time::TimeScale, ConfiguredScale,
};

use avenger_scales::scalar::Scalar as AvengerScalar;

pub fn to_configured_scale(
    scale_state: &ScaleState,
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<ConfiguredScale> {
    let mut configured = match &scale_state.scale_type {
        ScaleTypeSpec::Linear => LinearScale::configured((0.0, 1.0), (0.0, 1.0))
            .with_domain(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Log => LogScale::configured((1.0, 10.0), (0.0, 1.0))
            .with_domain(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Pow => PowScale::configured((0.0, 1.0), (0.0, 1.0))
            .with_domain(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Sqrt => PowScale::configured((0.0, 1.0), (0.0, 1.0))
            .with_domain(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Symlog => SymlogScale::configured((0.0, 1.0), (0.0, 1.0))
            .with_domain(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Band => BandScale::configured(scale_state.domain.clone(), (0.0, 1.0))
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Point => PointScale::configured(scale_state.domain.clone(), (0.0, 1.0))
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Ordinal => OrdinalScale::configured(scale_state.domain.clone())
            .with_range(scale_state.range.clone()),
        ScaleTypeSpec::Time | ScaleTypeSpec::Utc => {
            if scale_state.domain.len() < 2 {
                return Err(VegaFusionError::internal(format!(
                    "Time scale domain must have length 2, received {}",
                    scale_state.domain.len()
                )));
            }
            let d0 = scale_state.domain.slice(0, 1);
            let d1 = scale_state.domain.slice(1, 1);
            let range = array_interval_to_f32(&scale_state.range)?;
            TimeScale::configured((d0, d1), range)
                .with_domain(scale_state.domain.clone())
                .with_range(scale_state.range.clone())
        }
        unsupported => {
            return Err(VegaFusionError::internal(format!(
                "Scale type not supported for server-side evaluation in this phase: {unsupported:?}"
            )))
        }
    };

    let mapped_options = map_options(&scale_state.scale_type, &scale_state.options)?;
    for (key, value) in mapped_options {
        configured = configured.with_option(key, scalar_value_to_avenger_scalar(&value)?);
    }

    // Vega sqrt scales are pow scales with exponent 0.5.
    if matches!(scale_state.scale_type, ScaleTypeSpec::Sqrt) {
        configured = configured.with_option("exponent", 0.5_f32);
    }

    if matches!(scale_state.scale_type, ScaleTypeSpec::Time) {
        if !scale_state.options.contains_key("timezone") {
            let tz = tz_config
                .map(|tz| tz.local_tz.to_string())
                .unwrap_or_else(|| "UTC".to_string());
            configured = configured.with_option("timezone", tz);
        }
    } else if matches!(scale_state.scale_type, ScaleTypeSpec::Utc) {
        configured = configured.with_option("timezone", "UTC");
    }

    Ok(configured)
}

fn array_interval_to_f32(values: &vegafusion_common::arrow::array::ArrayRef) -> Result<(f32, f32)> {
    if values.len() < 2 {
        return Err(VegaFusionError::internal(format!(
            "Range interval must have at least two entries, received {}",
            values.len()
        )));
    }
    let lo = ScalarValue::try_from_array(values, 0)?;
    let hi = ScalarValue::try_from_array(values, 1)?;
    Ok((scalar_to_f32(&lo)?, scalar_to_f32(&hi)?))
}

fn scalar_to_f32(value: &ScalarValue) -> Result<f32> {
    match value {
        ScalarValue::Float32(Some(v)) => Ok(*v),
        ScalarValue::Float64(Some(v)) => Ok(*v as f32),
        ScalarValue::Int8(Some(v)) => Ok(*v as f32),
        ScalarValue::Int16(Some(v)) => Ok(*v as f32),
        ScalarValue::Int32(Some(v)) => Ok(*v as f32),
        ScalarValue::Int64(Some(v)) => Ok(*v as f32),
        ScalarValue::UInt8(Some(v)) => Ok(*v as f32),
        ScalarValue::UInt16(Some(v)) => Ok(*v as f32),
        ScalarValue::UInt32(Some(v)) => Ok(*v as f32),
        ScalarValue::UInt64(Some(v)) => Ok(*v as f32),
        _ => Err(VegaFusionError::internal(format!(
            "Expected numeric scalar value for range/domain option, received {value:?}"
        ))),
    }
}

pub fn scalar_value_to_avenger_scalar(value: &ScalarValue) -> Result<AvengerScalar> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(AvengerScalar::from(*v)),
        ScalarValue::Float32(Some(v)) => Ok(AvengerScalar::from(*v)),
        ScalarValue::Float64(Some(v)) => Ok(AvengerScalar::from(*v as f32)),
        ScalarValue::Int8(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::Int16(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::Int32(Some(v)) => Ok(AvengerScalar::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::UInt8(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::UInt16(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::UInt32(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::UInt64(Some(v)) => Ok(AvengerScalar::from(*v as i32)),
        ScalarValue::Utf8(Some(v)) => Ok(AvengerScalar::from(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Ok(AvengerScalar::from(v.clone())),
        ScalarValue::Utf8View(Some(v)) => Ok(AvengerScalar::from(v.clone())),
        _ => Err(VegaFusionError::internal(format!(
            "Unsupported scale option scalar type: {value:?}"
        ))),
    }
}

fn map_options(
    scale_type: &ScaleTypeSpec,
    options: &HashMap<String, ScalarValue>,
) -> Result<HashMap<String, ScalarValue>> {
    let mut mapped: HashMap<String, ScalarValue> = HashMap::new();
    for (key, value) in options {
        let Some(mapped_key) = map_option_name(scale_type, key)? else {
            continue;
        };
        mapped.insert(mapped_key.to_string(), value.clone());
    }
    Ok(mapped)
}

fn map_option_name<'a>(scale_type: &ScaleTypeSpec, key: &'a str) -> Result<Option<&'a str>> {
    // Ignored in this phase: these options are consumed by range/domain construction or
    // not currently modeled by avenger's runtime scale options.
    if matches!(
        key,
        "bins" | "interpolate" | "interpolate_gamma" | "domain_implicit"
    ) {
        return Ok(None);
    }

    let mapped = match scale_type {
        ScaleTypeSpec::Linear => match key {
            "clamp" | "range_offset" | "round" | "nice" | "default" | "clip_padding_lower"
            | "clip_padding_upper" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Log => match key {
            "base" | "clamp" | "range_offset" | "round" | "nice" | "default"
            | "clip_padding_lower" | "clip_padding_upper" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Pow | ScaleTypeSpec::Sqrt => match key {
            "exponent" | "clamp" | "range_offset" | "round" | "nice" | "default"
            | "clip_padding_lower" | "clip_padding_upper" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Symlog => match key {
            "constant" | "clamp" | "range_offset" | "round" | "nice" | "default"
            | "clip_padding_lower" | "clip_padding_upper" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Time | ScaleTypeSpec::Utc => match key {
            "timezone" | "nice" | "interval" | "week_start" | "locale" | "default" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Band => match key {
            "align" | "band" | "padding" | "padding_inner" | "padding_inner_px"
            | "padding_outer" | "padding_outer_px" | "round" | "range_offset"
            | "clip_padding_lower" | "clip_padding_upper" | "band_n" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Point => match key {
            "align" | "padding" | "round" | "range_offset" | "clip_padding_lower"
            | "clip_padding_upper" => Some(key),
            _ => None,
        },
        ScaleTypeSpec::Ordinal => match key {
            "default" => Some(key),
            _ => None,
        },
        // Unsupported scales in this phase are rejected earlier.
        _ => None,
    };

    if let Some(mapped) = mapped {
        Ok(Some(mapped))
    } else {
        Err(VegaFusionError::internal(format!(
            "Unsupported scale option for server-side {scale_type:?} scale: {key}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use vegafusion_common::arrow::array::{ArrayRef, Float64Array, TimestampMillisecondArray};
    use vegafusion_core::task_graph::scale_state::ScaleState;

    fn numeric_interval(values: [f64; 2]) -> vegafusion_common::arrow::array::ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    #[test]
    fn test_bins_option_is_ignored_for_linear_scale() {
        let mut options = HashMap::new();
        options.insert(
            "bins".to_string(),
            ScalarValue::from(vec![
                ("start", ScalarValue::from(0.0)),
                ("step", ScalarValue::from(10.0)),
                ("stop", ScalarValue::from(100.0)),
            ]),
        );

        let scale_state = ScaleState {
            scale_type: ScaleTypeSpec::Linear,
            domain: numeric_interval([0.0, 100.0]),
            range: numeric_interval([0.0, 200.0]),
            options,
        };

        let configured = to_configured_scale(&scale_state, &None);
        assert!(configured.is_ok());
    }

    #[test]
    fn test_unknown_option_errors_explicitly() {
        let mut options = HashMap::new();
        options.insert("foo".to_string(), ScalarValue::from(1.0));

        let scale_state = ScaleState {
            scale_type: ScaleTypeSpec::Linear,
            domain: numeric_interval([0.0, 100.0]),
            range: numeric_interval([0.0, 200.0]),
            options,
        };

        let err = to_configured_scale(&scale_state, &None).unwrap_err();
        assert!(format!("{err}").contains("Unsupported scale option for server-side"));
    }

    #[test]
    fn test_time_scale_maps_timestamp_inputs_to_range() {
        let domain: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![1317441600000_i64, 1451624400000_i64]),
        );
        let range: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64, 200.0_f64]));

        let state = ScaleState {
            scale_type: ScaleTypeSpec::Time,
            domain,
            range,
            options: HashMap::new(),
        };
        let configured = to_configured_scale(&state, &None).unwrap();

        let input: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1325394000000_i64]));
        let output = configured.scale(&input).unwrap();

        let output_scalar = ScalarValue::try_from_array(output.as_ref(), 0).unwrap();
        let x = match output_scalar {
            ScalarValue::Float32(Some(v)) => v,
            ScalarValue::Float64(Some(v)) => v as f32,
            other => panic!("Unexpected time-scale output scalar: {other:?}"),
        };

        assert!((x - 11.853_f32).abs() < 0.5, "unexpected scaled x value: {x}");
    }
}
