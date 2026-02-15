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
    let domain = resolve_domain(scale, &scale_type, config)?;
    let range = resolve_range(scale, config)?;
    let options = resolve_options(scale, config)?;
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
fn resolve_range(scale: &ScaleSpec, config: &CompilationConfig) -> Result<ArrayRef> {
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
                    range_from_dimension(range_name, config)
                } else {
                    Err(VegaFusionError::internal(format!(
                        "Unsupported named range {range_name:?} in this phase"
                    )))
                }
            } else {
                json_value_to_array(value)
            }
        }
    }
}

#[cfg(feature = "scales")]
fn resolve_options(
    scale: &ScaleSpec,
    config: &CompilationConfig,
) -> Result<HashMap<String, ScalarValue>> {
    let mut options = HashMap::new();

    for (key, value) in &scale.extra {
        let resolved = if let Some(signal_expr) = value
            .as_object()
            .and_then(|obj| obj.get("signal"))
            .and_then(|v| v.as_str())
        {
            eval_signal_expr(signal_expr, config)?
        } else {
            ScalarValue::from_json(value)?
        };

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
fn range_from_dimension(range_name: &str, config: &CompilationConfig) -> Result<ArrayRef> {
    let dim = config.signal_scope.get(range_name).ok_or_else(|| {
        VegaFusionError::internal(format!(
            "Range {range_name:?} requires a signal named {range_name:?} in scope"
        ))
    })?;

    let dim = scalar_to_f64(dim)?;
    let values = match range_name {
        "width" => vec![ScalarValue::from(0.0_f64), ScalarValue::from(dim)],
        "height" => vec![ScalarValue::from(dim), ScalarValue::from(0.0_f64)],
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
