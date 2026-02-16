use crate::task_graph::task::TaskCall;
use crate::task_graph::timezone::RuntimeTzConfig;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::proto::gen::tasks::ScaleTask;
use vegafusion_core::task_graph::task_value::TaskValue;

#[cfg(feature = "scales")]
use crate::data::tasks::build_compilation_config;
#[cfg(feature = "scales")]
use crate::expression::compiler::compile;
#[cfg(feature = "scales")]
use crate::expression::compiler::config::CompilationConfig;
#[cfg(feature = "scales")]
use crate::expression::compiler::utils::ExprHelpers;
#[cfg(feature = "scales")]
use crate::scale::vega_defaults::apply_vega_domain_defaults;
#[cfg(feature = "scales")]
use datafusion_common::ScalarValue;
#[cfg(feature = "scales")]
use std::collections::HashSet;
#[cfg(feature = "scales")]
use vegafusion_common::arrow::array::{new_empty_array, Array, ArrayRef, FixedSizeListArray};
#[cfg(feature = "scales")]
use vegafusion_common::arrow::datatypes::DataType;
#[cfg(feature = "scales")]
use vegafusion_common::data::scalar::ScalarValueHelpers;
#[cfg(feature = "scales")]
use vegafusion_common::escape::unescape_field;
#[cfg(feature = "scales")]
use vegafusion_core::expression::parser::parse;
#[cfg(feature = "scales")]
use vegafusion_core::spec::scale::{
    ScaleArrayElementSpec, ScaleBinsSpec, ScaleDataReferenceOrSignalSpec, ScaleDomainSpec,
    ScaleRangeSpec, ScaleSpec, ScaleTypeSpec,
};
#[cfg(feature = "scales")]
use vegafusion_core::task_graph::task::TaskDependencies;
#[cfg(feature = "scales")]
use vegafusion_core::task_graph::{scale_state::ScaleState, task::InputVariable};

#[cfg(feature = "scales")]
#[async_trait]
impl TaskCall for ScaleTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        let scale_spec: ScaleSpec = serde_json::from_str(&self.spec)?;
        let input_vars: Vec<InputVariable> = self.input_vars();
        let config = build_compilation_config(&input_vars, values, tz_config);
        let scale_state = resolve_scale_state(&scale_spec, &config)?;
        Ok((TaskValue::Scale(scale_state), Vec::new()))
    }
}

#[cfg(not(feature = "scales"))]
#[async_trait]
impl TaskCall for ScaleTask {
    async fn eval(
        &self,
        _values: &[TaskValue],
        _tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        _ctx: Arc<SessionContext>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        Err(VegaFusionError::internal(
            "Server-side scale evaluation requires the vegafusion-runtime `scales` feature",
        ))
    }
}

#[cfg(feature = "scales")]
fn resolve_scale_state(scale: &ScaleSpec, config: &CompilationConfig) -> Result<ScaleState> {
    let scale_type = scale.type_.clone().unwrap_or_default();
    let mut options = resolve_options(scale, config)?;

    let domain_raw = resolve_domain_raw_option(&options)?;
    let domain_from_raw = domain_raw.is_some();
    let domain = match domain_raw {
        Some(raw_domain) => raw_domain,
        None => resolve_domain(scale, &scale_type, config)?,
    };

    let range = resolve_range(scale, &scale_type, domain.len(), &options, config)?;
    let domain =
        apply_vega_domain_defaults(&scale_type, domain, &range, &mut options, domain_from_raw)?;

    Ok(ScaleState {
        scale_type,
        domain,
        range,
        options,
    })
}

#[cfg(feature = "scales")]
fn resolve_domain(
    scale: &ScaleSpec,
    scale_type: &ScaleTypeSpec,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let domain = scale.domain.as_ref().ok_or_else(|| {
        VegaFusionError::internal(format!(
            "Scale {} is missing a domain definition",
            scale.name
        ))
    })?;

    match domain {
        ScaleDomainSpec::Array(values) => scale_array_elements_to_array(values, config),
        ScaleDomainSpec::Signal(signal_expr) => signal_expr_to_array(&signal_expr.signal, config),
        ScaleDomainSpec::Value(value) => json_value_to_array(value),
        ScaleDomainSpec::FieldReference(reference) => domain_from_data_fields(
            scale_type,
            &[(reference.data.as_str(), reference.field.as_str())],
            config,
        ),
        ScaleDomainSpec::FieldsReference(fields_reference) => {
            let data_name = fields_reference.data.as_ref().ok_or_else(|| {
                VegaFusionError::internal(format!(
                    "Scale {} fields-reference domain is missing `data`",
                    scale.name
                ))
            })?;
            let fields = fields_reference
                .fields
                .iter()
                .map(|f| (data_name.as_str(), f.as_str()))
                .collect::<Vec<_>>();
            domain_from_data_fields(scale_type, fields.as_slice(), config)
        }
        ScaleDomainSpec::FieldsReferences(fields_references) => {
            let mut arrays = Vec::new();
            for reference_or_signal in &fields_references.fields {
                match reference_or_signal {
                    ScaleDataReferenceOrSignalSpec::Reference(reference) => {
                        arrays.push(lookup_data_column(
                            config,
                            reference.data.as_str(),
                            reference.field.as_str(),
                        )?)
                    }
                    ScaleDataReferenceOrSignalSpec::Signal(signal_expr) => {
                        arrays.push(signal_expr_to_array(&signal_expr.signal, config)?)
                    }
                }
            }
            merge_domain_arrays(scale_type, arrays)
        }
        ScaleDomainSpec::FieldsSignals(fields_signals) => {
            let arrays = fields_signals
                .fields
                .iter()
                .map(|signal_expr| signal_expr_to_array(&signal_expr.signal, config))
                .collect::<Result<Vec<_>>>()?;
            merge_domain_arrays(scale_type, arrays)
        }
        unsupported => Err(VegaFusionError::internal(format!(
            "Unsupported scale domain form in this phase: {unsupported:?}"
        ))),
    }
}

#[cfg(feature = "scales")]
fn resolve_range(
    scale: &ScaleSpec,
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
    options: &HashMap<String, ScalarValue>,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    if let Some(step) = resolve_range_step(scale, options, config)? {
        return range_from_step(scale_type, domain_len, step, options);
    }

    let range = scale.range.as_ref().ok_or_else(|| {
        VegaFusionError::internal(format!(
            "Scale {} is missing a range definition",
            scale.name
        ))
    })?;

    match range {
        ScaleRangeSpec::Array(values) => scale_array_elements_to_array(values, config),
        ScaleRangeSpec::Signal(signal_expr) => signal_expr_to_array(&signal_expr.signal, config),
        ScaleRangeSpec::Reference(reference) => {
            lookup_data_column(config, reference.data.as_str(), reference.field.as_str())
        }
        ScaleRangeSpec::Value(value) => {
            if let Some(range_name) = value.as_str() {
                if matches!(range_name, "width" | "height") {
                    range_from_dimension(range_name, scale_type, config)
                } else {
                    Err(VegaFusionError::internal(format!(
                        "Unsupported named range {range_name:?} in this phase"
                    )))
                }
            } else if value.as_object().and_then(|obj| obj.get("step")).is_some() {
                Err(VegaFusionError::internal(format!(
                    "Scale {} has range.step but it could not be resolved",
                    scale.name
                )))
            } else if value.is_object() {
                Err(VegaFusionError::internal(format!(
                    "Unsupported range object in this phase for scale {}: {value:?}",
                    scale.name
                )))
            } else {
                json_value_to_array(value)
            }
        }
    }
}

#[cfg(feature = "scales")]
fn resolve_range_step(
    scale: &ScaleSpec,
    options: &HashMap<String, ScalarValue>,
    config: &CompilationConfig,
) -> Result<Option<f64>> {
    if let Some(step) = options.get("range_step").and_then(non_null_option_scalar) {
        return Ok(Some(scalar_to_f64(step)?));
    }

    if let Some(ScaleRangeSpec::Value(value)) = scale.range.as_ref() {
        if let Some(step_value) = value.as_object().and_then(|obj| obj.get("step")) {
            let step_scalar = eval_json_or_signal_scalar(step_value, config)?;
            if step_scalar.is_null() {
                return Ok(None);
            }
            return Ok(Some(scalar_to_f64(&step_scalar)?));
        }
    }

    Ok(None)
}

#[cfg(feature = "scales")]
fn range_from_step(
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
    step: f64,
    options: &HashMap<String, ScalarValue>,
) -> Result<ArrayRef> {
    if !matches!(scale_type, ScaleTypeSpec::Band | ScaleTypeSpec::Point) {
        return Err(VegaFusionError::internal(
            "Only band and point scales support rangeStep",
        ));
    }

    let padding = option_f64(options, "padding", 0.0)?;
    let padding_outer = option_f64(options, "padding_outer", padding)?;
    let padding_inner = if matches!(scale_type, ScaleTypeSpec::Point) {
        1.0
    } else {
        option_f64(options, "padding_inner", padding)?
    };

    let count = domain_len as f64;
    let band_space = if count > 0.0 {
        (count - padding_inner + padding_outer * 2.0).max(1.0)
    } else {
        0.0
    };
    let extent = step * band_space;

    Ok(ScalarValue::iter_to_array(vec![
        ScalarValue::from(0.0_f64),
        ScalarValue::from(extent),
    ])?)
}

#[cfg(feature = "scales")]
fn resolve_domain_raw_option(options: &HashMap<String, ScalarValue>) -> Result<Option<ArrayRef>> {
    let Some(domain_raw) = options.get("domain_raw").and_then(non_null_option_scalar) else {
        return Ok(None);
    };

    Ok(Some(scalar_to_array_or_singleton(domain_raw)?))
}

#[cfg(feature = "scales")]
fn non_null_option_scalar(scalar: &ScalarValue) -> Option<&ScalarValue> {
    if scalar.is_null() {
        None
    } else {
        Some(scalar)
    }
}

#[cfg(feature = "scales")]
fn option_f64(options: &HashMap<String, ScalarValue>, key: &str, default: f64) -> Result<f64> {
    if let Some(value) = options.get(key).and_then(non_null_option_scalar) {
        scalar_to_f64(value)
    } else {
        Ok(default)
    }
}

#[cfg(feature = "scales")]
fn eval_json_or_signal_scalar(
    value: &serde_json::Value,
    config: &CompilationConfig,
) -> Result<ScalarValue> {
    if let Some(signal_expr) = value
        .as_object()
        .and_then(|obj| obj.get("signal"))
        .and_then(|v| v.as_str())
    {
        eval_signal_expr(signal_expr, config)
    } else {
        ScalarValue::from_json(value).map_err(VegaFusionError::from)
    }
}

#[cfg(feature = "scales")]
fn resolve_options(
    scale: &ScaleSpec,
    config: &CompilationConfig,
) -> Result<HashMap<String, ScalarValue>> {
    let mut options = HashMap::new();

    for (key, value) in &scale.extra {
        let resolved = eval_json_or_signal_scalar(value, config)?;
        options.insert(normalize_option_name(key), resolved);
    }

    // `bins` can participate in scale behavior for some scale types.
    if let Some(bins) = &scale.bins {
        let bins_value = match bins {
            ScaleBinsSpec::Signal(signal_expr) => eval_signal_expr(&signal_expr.signal, config)?,
            ScaleBinsSpec::Array(arr) => {
                let arr = scale_array_elements_to_array(arr, config)?;
                ScalarValue::List(Arc::new(
                    datafusion_common::utils::SingleRowListArrayBuilder::new(arr)
                        .with_nullable(true)
                        .build_list_array(),
                ))
            }
            ScaleBinsSpec::Value(value) => ScalarValue::from_json(value)?,
        };
        options.insert("bins".to_string(), bins_value);
    }

    Ok(options)
}

#[cfg(feature = "scales")]
fn domain_from_data_fields(
    scale_type: &ScaleTypeSpec,
    fields: &[(&str, &str)],
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let arrays = fields
        .iter()
        .map(|(data_name, field_name)| lookup_data_column(config, data_name, field_name))
        .collect::<Result<Vec<_>>>()?;
    merge_domain_arrays(scale_type, arrays)
}

#[cfg(feature = "scales")]
fn merge_domain_arrays(scale_type: &ScaleTypeSpec, arrays: Vec<ArrayRef>) -> Result<ArrayRef> {
    if arrays.is_empty() {
        return Err(VegaFusionError::internal(
            "Scale domain references resolved to an empty set of arrays",
        ));
    }

    if is_continuous_scale(scale_type) {
        let mut min_val: Option<ScalarValue> = None;
        let mut max_val: Option<ScalarValue> = None;

        for arr in arrays {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    continue;
                }
                let value = ScalarValue::try_from_array(arr.as_ref(), i)?;
                if min_val
                    .as_ref()
                    .map(|min_val| value.partial_cmp(min_val) == Some(std::cmp::Ordering::Less))
                    .unwrap_or(true)
                {
                    min_val = Some(value.clone());
                }
                if max_val
                    .as_ref()
                    .map(|max_val| value.partial_cmp(max_val) == Some(std::cmp::Ordering::Greater))
                    .unwrap_or(true)
                {
                    max_val = Some(value);
                }
            }
        }

        match (min_val, max_val) {
            (Some(min_val), Some(max_val)) => {
                Ok(ScalarValue::iter_to_array(vec![min_val, max_val])?)
            }
            _ => Ok(new_empty_array(&DataType::Float64)),
        }
    } else {
        let mut seen: HashSet<ScalarValue> = HashSet::new();
        let mut values = Vec::new();
        for arr in arrays {
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    continue;
                }
                let value = ScalarValue::try_from_array(arr.as_ref(), i)?;
                if seen.insert(value.clone()) {
                    values.push(value);
                }
            }
        }

        if values.is_empty() {
            Ok(new_empty_array(&DataType::Utf8))
        } else {
            Ok(ScalarValue::iter_to_array(values)?)
        }
    }
}

#[cfg(feature = "scales")]
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

#[cfg(feature = "scales")]
fn is_discrete_scale(scale_type: &ScaleTypeSpec) -> bool {
    matches!(
        scale_type,
        ScaleTypeSpec::Ordinal | ScaleTypeSpec::Band | ScaleTypeSpec::Point
    )
}

#[cfg(feature = "scales")]
fn lookup_data_column(
    config: &CompilationConfig,
    data_name: &str,
    field_name: &str,
) -> Result<ArrayRef> {
    let table = config.data_scope.get(data_name).ok_or_else(|| {
        VegaFusionError::internal(format!(
            "No dataset named {data_name} available when evaluating scale state"
        ))
    })?;

    let rb = table.to_record_batch()?;
    let mut candidates = vec![field_name.to_string()];
    let unescaped = unescape_field(field_name);
    if unescaped != field_name {
        candidates.push(unescaped);
    }

    for candidate in candidates {
        if let Some((index, _)) = rb.schema().column_with_name(candidate.as_str()) {
            return Ok(rb.column(index).clone());
        }
    }

    Err(VegaFusionError::internal(format!(
        "No field {field_name:?} in dataset {data_name:?}. Available fields: {:?}",
        rb.schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    )))
}

#[cfg(feature = "scales")]
fn eval_signal_expr(expr_str: &str, config: &CompilationConfig) -> Result<ScalarValue> {
    let expression = parse(expr_str)?;
    let compiled = compile(&expression, config, None)?;
    compiled.eval_to_scalar()
}

#[cfg(feature = "scales")]
fn signal_expr_to_array(expr_str: &str, config: &CompilationConfig) -> Result<ArrayRef> {
    let scalar = eval_signal_expr(expr_str, config)?;
    scalar_to_array_or_singleton(&scalar)
}

#[cfg(feature = "scales")]
fn scalar_to_array_or_singleton(scalar: &ScalarValue) -> Result<ArrayRef> {
    match scalar {
        ScalarValue::List(arr) => {
            if arr.is_empty() {
                Ok(new_empty_array(&arr.value_type()))
            } else {
                Ok(arr.value(0))
            }
        }
        ScalarValue::LargeList(arr) => {
            if arr.is_empty() {
                Ok(new_empty_array(&arr.value_type()))
            } else {
                Ok(arr.value(0))
            }
        }
        ScalarValue::FixedSizeList(arr) => {
            if arr.is_empty() {
                Ok(new_empty_array(&arr.value_type()))
            } else {
                let arr = arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        VegaFusionError::internal("Failed to downcast fixed-size list")
                    })?;
                Ok(arr.value(0))
            }
        }
        _ => Ok(scalar.to_array()?),
    }
}

#[cfg(feature = "scales")]
fn scale_array_elements_to_array(
    values: &[ScaleArrayElementSpec],
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let scalars = values
        .iter()
        .map(|value| match value {
            ScaleArrayElementSpec::Signal(signal_expr) => {
                eval_signal_expr(&signal_expr.signal, config)
            }
            ScaleArrayElementSpec::Value(value) => {
                ScalarValue::from_json(value).map_err(VegaFusionError::from)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    if scalars.is_empty() {
        Ok(new_empty_array(&DataType::Float64))
    } else {
        Ok(ScalarValue::iter_to_array(scalars)?)
    }
}

#[cfg(feature = "scales")]
fn json_value_to_array(value: &serde_json::Value) -> Result<ArrayRef> {
    if let Some(arr) = value.as_array() {
        let scalars = arr
            .iter()
            .map(|v| ScalarValue::from_json(v).map_err(VegaFusionError::from))
            .collect::<Result<Vec<_>>>()?;
        if scalars.is_empty() {
            Ok(new_empty_array(&DataType::Float64))
        } else {
            Ok(ScalarValue::iter_to_array(scalars)?)
        }
    } else {
        let scalar = ScalarValue::from_json(value)?;
        scalar_to_array_or_singleton(&scalar)
    }
}

#[cfg(feature = "scales")]
fn range_from_dimension(
    range_name: &str,
    scale_type: &ScaleTypeSpec,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let dim = config.signal_scope.get(range_name).ok_or_else(|| {
        VegaFusionError::internal(format!(
            "Range {range_name:?} requires a signal named {range_name:?} in scope"
        ))
    })?;

    let dim = scalar_to_f64(dim)?;
    let values = match range_name {
        "width" => vec![ScalarValue::from(0.0_f64), ScalarValue::from(dim)],
        "height" => {
            if is_discrete_scale(scale_type) {
                vec![ScalarValue::from(0.0_f64), ScalarValue::from(dim)]
            } else {
                vec![ScalarValue::from(dim), ScalarValue::from(0.0_f64)]
            }
        }
        _ => {
            return Err(VegaFusionError::internal(format!(
                "Unsupported range dimension {range_name:?}"
            )))
        }
    };
    Ok(ScalarValue::iter_to_array(values)?)
}

#[cfg(feature = "scales")]
fn scalar_to_f64(value: &ScalarValue) -> Result<f64> {
    match value {
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
        _ => Err(VegaFusionError::internal(format!(
            "Expected numeric scalar value, received {value:?}"
        ))),
    }
}

#[cfg(feature = "scales")]
fn normalize_option_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                normalized.push('_');
            }
            normalized.push(c.to_ascii_lowercase());
        } else {
            normalized.push(c);
        }
    }
    normalized
}

#[cfg(all(test, feature = "scales"))]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;
    use serde_json::json;

    fn config_with_dimensions(width: f64, height: f64) -> CompilationConfig {
        let mut config = CompilationConfig::default();
        config
            .signal_scope
            .insert("width".to_string(), ScalarValue::from(width));
        config
            .signal_scope
            .insert("height".to_string(), ScalarValue::from(height));
        config
    }

    fn scalar_domain_f64(value: &ScalarValue) -> f64 {
        match value {
            ScalarValue::Date32(Some(days)) => (*days as f64) * 86_400_000.0,
            ScalarValue::Date64(Some(ms)) => *ms as f64,
            ScalarValue::TimestampSecond(Some(v), _) => (*v as f64) * 1000.0,
            ScalarValue::TimestampMillisecond(Some(v), _) => *v as f64,
            ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as f64) / 1000.0,
            ScalarValue::TimestampNanosecond(Some(v), _) => (*v as f64) / 1_000_000.0,
            _ => scalar_to_f64(value).unwrap(),
        }
    }

    fn scalar_f64(values: &ArrayRef) -> (f64, f64) {
        let lo = ScalarValue::try_from_array(values, 0).unwrap();
        let hi = ScalarValue::try_from_array(values, 1).unwrap();
        (scalar_domain_f64(&lo), scalar_domain_f64(&hi))
    }

    #[test]
    fn test_default_zero_for_linear_pow_sqrt() {
        for scale_type in ["linear", "pow", "sqrt"] {
            let scale: ScaleSpec = serde_json::from_value(json!({
                "name": "x",
                "type": scale_type,
                "domain": [2, 5],
                "range": [0, 100]
            }))
            .unwrap();
            let state = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap();
            let (lo, hi) = scalar_f64(&state.domain);
            assert_eq!(lo, 0.0, "scale type {scale_type}");
            assert_eq!(hi, 5.0, "scale type {scale_type}");
            assert!(!state.options.contains_key("zero"));
        }
    }

    #[test]
    fn test_explicit_zero_false_overrides_default() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "linear",
            "domain": [2, 5],
            "range": [0, 100],
            "zero": false
        }))
        .unwrap();
        let state = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap();
        let (lo, hi) = scalar_f64(&state.domain);
        assert_eq!(lo, 2.0);
        assert_eq!(hi, 5.0);
    }

    #[test]
    fn test_height_range_orientation_discrete_and_continuous() {
        let config = config_with_dimensions(400.0, 200.0);

        let discrete: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": ["A", "B"],
            "range": "height"
        }))
        .unwrap();
        let continuous: ScaleSpec = serde_json::from_value(json!({
            "name": "y",
            "type": "linear",
            "domain": [0, 1],
            "range": "height"
        }))
        .unwrap();

        let d_state = resolve_scale_state(&discrete, &config).unwrap();
        let c_state = resolve_scale_state(&continuous, &config).unwrap();

        assert_eq!(scalar_f64(&d_state.range), (0.0, 200.0));
        assert_eq!(scalar_f64(&c_state.range), (200.0, 0.0));
    }

    #[test]
    fn test_range_step_for_band_and_point() {
        let band: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": ["A", "B", "C"],
            "range": {"step": 20},
            "paddingInner": 0.1,
            "paddingOuter": 0.2
        }))
        .unwrap();
        let point: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "point",
            "domain": ["A", "B", "C"],
            "range": {"step": 20},
            "padding": 0.5
        }))
        .unwrap();

        let band_state = resolve_scale_state(&band, &CompilationConfig::default()).unwrap();
        let point_state = resolve_scale_state(&point, &CompilationConfig::default()).unwrap();

        assert_eq!(scalar_f64(&band_state.range), (0.0, 66.0));
        assert_eq!(scalar_f64(&point_state.range), (0.0, 60.0));
        assert!(!band_state.options.contains_key("range_step"));
        assert!(!point_state.options.contains_key("range_step"));
    }

    #[test]
    fn test_domain_raw_precedence_and_bypass() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "linear",
            "range": [0, 100],
            "domainRaw": [2, 5],
            "zero": true,
            "padding": 10,
            "domainMin": 0,
            "domainMax": 10
        }))
        .unwrap();

        let state = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap();
        assert_eq!(scalar_f64(&state.domain), (2.0, 5.0));
        assert!(!state.options.contains_key("domain_raw"));
    }

    #[test]
    fn test_domain_min_max_override() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "linear",
            "domain": [2, 5],
            "range": [0, 100],
            "domainMin": 1,
            "domainMax": 10,
            "zero": false
        }))
        .unwrap();

        let state = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap();
        assert_eq!(scalar_f64(&state.domain), (1.0, 10.0));
        assert!(!state.options.contains_key("domain_min"));
        assert!(!state.options.contains_key("domain_max"));
    }

    #[test]
    fn test_domain_mid_errors() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "linear",
            "domain": [2, 5],
            "range": [0, 100],
            "domainMid": 3
        }))
        .unwrap();

        let err = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap_err();
        assert!(err.to_string().contains(
            "domainMid is not yet supported for server-evaluated piecewise continuous scales"
        ));
    }

    #[test]
    fn test_continuous_padding_changes_domain() {
        let config = CompilationConfig::default();
        for scale in [
            json!({"name":"x","type":"linear","domain":[2,5],"range":[100,0],"padding":10}),
            json!({"name":"x","type":"log","domain":[2,5],"range":[100,0],"padding":10}),
            json!({"name":"x","type":"pow","domain":[2,5],"range":[100,0],"padding":10,"zero":false,"exponent":2}),
            json!({"name":"x","type":"sqrt","domain":[2,5],"range":[100,0],"padding":10,"zero":false}),
            json!({"name":"x","type":"symlog","domain":[2,5],"range":[100,0],"padding":10,"constant":1}),
            json!({"name":"x","type":"time","domain":[0,1000],"range":[100,0],"padding":10}),
            json!({"name":"x","type":"utc","domain":[0,1000],"range":[100,0],"padding":10}),
        ] {
            let spec: ScaleSpec = serde_json::from_value(scale).unwrap();
            let state = resolve_scale_state(&spec, &config).unwrap();
            let (lo, hi) = scalar_f64(&state.domain);
            if matches!(spec.type_, Some(ScaleTypeSpec::Time | ScaleTypeSpec::Utc)) {
                assert!(lo < 0.0 || hi > 1000.0);
            } else {
                assert!(lo < 2.0 || hi > 5.0);
            }
        }
    }
}
