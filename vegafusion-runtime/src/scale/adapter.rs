use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_common::ScalarValue;
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

    for (key, value) in &scale_state.options {
        configured = configured.with_option(key.clone(), scalar_value_to_avenger_scalar(value)?);
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
