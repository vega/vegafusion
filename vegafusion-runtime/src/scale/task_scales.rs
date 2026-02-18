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

use crate::data::tasks::build_compilation_config;
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::ExprHelpers;
use crate::scale::vega_defaults::apply_vega_domain_defaults;
use crate::scale::vega_schemes::{
    decode_continuous_scheme, default_named_range, lookup_scheme, NamedRange, SchemePalette,
};
use crate::scale::DISCRETE_NULL_SENTINEL;
use datafusion_common::ScalarValue;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::RangeInclusive;
use vegafusion_common::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, AsArray, FixedSizeListArray, StringArray,
};
use vegafusion_common::arrow::compute::cast;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::expression::parser::parse;
use vegafusion_core::spec::scale::{
    ScaleArrayElementSpec, ScaleBinsSpec, ScaleDataReferenceOrSignalSpec, ScaleDataReferenceSort,
    ScaleDataReferenceSortParameters, ScaleDomainSpec, ScaleFieldReferenceSpec, ScaleRangeSpec,
    ScaleSpec, ScaleTypeSpec, ScaleVecStringsSpec,
};
use vegafusion_core::spec::transform::aggregate::AggregateOpSpec;
use vegafusion_core::spec::values::SortOrderSpec;
use vegafusion_core::task_graph::task::TaskDependencies;
use vegafusion_core::task_graph::{scale_state::ScaleState, task::InputVariable};

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

fn resolve_scale_state(scale: &ScaleSpec, config: &CompilationConfig) -> Result<ScaleState> {
    let scale_type = scale.type_.clone().unwrap_or_default();
    let mut options = resolve_options(scale, config)?;
    let implicit_zero_allowed = domain_allows_implicit_zero(scale);

    let domain_raw = resolve_domain_raw_option(&options)?;
    let domain_from_raw = domain_raw.is_some();
    let domain = match domain_raw {
        Some(raw_domain) => raw_domain,
        None => resolve_domain(scale, &scale_type, config)?,
    };
    let domain = normalize_discrete_domain_for_avenger(&scale_type, domain)?;
    let domain = encode_discrete_null_domain(&scale_type, domain)?;

    let mut range = resolve_range(scale, &scale_type, domain.len(), &options, config)?;
    range = apply_reverse_option(range, &mut options)?;
    consume_range_construction_options(&mut options);
    let domain = apply_vega_domain_defaults(
        &scale_type,
        domain,
        &range,
        &mut options,
        domain_from_raw,
        implicit_zero_allowed,
    )?;

    if std::env::var_os("VF_DEBUG_SCALE_STATE").is_some() && scale.name == "x" {
        let d0 = if !domain.is_empty() {
            Some(ScalarValue::try_from_array(&domain, 0)?)
        } else {
            None
        };
        let d1 = if domain.len() > 1 {
            Some(ScalarValue::try_from_array(&domain, domain.len() - 1)?)
        } else {
            None
        };
        eprintln!(
            "scale_state {} type={:?} domain_dtype={:?} range_dtype={:?} domain_len={} endpoints={:?} {:?}",
            scale.name,
            scale_type,
            domain.data_type(),
            range.data_type(),
            domain.len(),
            d0,
            d1
        );
    }

    Ok(ScaleState {
        scale_type,
        domain,
        range,
        options,
    })
}

fn domain_allows_implicit_zero(scale: &ScaleSpec) -> bool {
    matches!(
        scale.domain.as_ref(),
        Some(
            ScaleDomainSpec::FieldReference(_)
                | ScaleDomainSpec::FieldsReference(_)
                | ScaleDomainSpec::FieldsReferences(_)
        )
    )
}

fn normalize_discrete_domain_for_avenger(
    scale_type: &ScaleTypeSpec,
    domain: ArrayRef,
) -> Result<ArrayRef> {
    if !is_discrete_scale(scale_type) || domain.null_count() > 0 {
        return Ok(domain);
    }

    let needs_temporal_key_normalization = matches!(
        domain.data_type(),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    );
    let needs_unsigned_key_normalization = matches!(
        domain.data_type(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    );
    let needs_boolean_key_normalization = matches!(domain.data_type(), DataType::Boolean);

    if !needs_temporal_key_normalization
        && !needs_unsigned_key_normalization
        && !needs_boolean_key_normalization
    {
        return Ok(domain);
    }

    if needs_boolean_key_normalization {
        return cast(domain.as_ref(), &DataType::Utf8).map_err(|err| {
            VegaFusionError::internal(format!(
                "Failed to normalize boolean discrete domain to Utf8: {err}"
            ))
        });
    }

    cast(domain.as_ref(), &DataType::Int64).or_else(|err| {
        // Fall back to Utf8 if Int64 coercion fails (for example due to UInt64 overflow).
        cast(domain.as_ref(), &DataType::Utf8).map_err(|fallback_err| {
            VegaFusionError::internal(format!(
                "Failed to normalize discrete domain to Int64 ({err}); fallback Utf8 cast also failed: {fallback_err}"
            ))
        })
    })
}

fn encode_discrete_null_domain(scale_type: &ScaleTypeSpec, domain: ArrayRef) -> Result<ArrayRef> {
    if !is_discrete_scale(scale_type) || domain.null_count() == 0 {
        return Ok(domain);
    }

    let casted = cast(domain.as_ref(), &DataType::Utf8).map_err(|err| {
        VegaFusionError::internal(format!(
            "Failed to cast discrete domain with null values to Utf8: {err}"
        ))
    })?;
    let strings = casted.as_string::<i32>();
    let encoded = (0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                Some(DISCRETE_NULL_SENTINEL.to_string())
            } else {
                Some(strings.value(i).to_string())
            }
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(StringArray::from(encoded)))
}

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
        ScaleDomainSpec::FieldsVecStrings(vec_strings) => domain_from_vec_strings(vec_strings),
        ScaleDomainSpec::FieldReference(reference) => {
            domain_from_field_reference(scale_type, reference, config)
        }
        ScaleDomainSpec::FieldsReference(fields_reference) => {
            let data_name = fields_reference.data.as_ref().ok_or_else(|| {
                VegaFusionError::internal(format!(
                    "Scale {} fields-reference domain is missing `data`",
                    scale.name
                ))
            })?;
            let references = fields_reference
                .fields
                .iter()
                .map(|f| ScaleFieldReferenceSpec {
                    data: data_name.clone(),
                    field: f.clone(),
                    sort: None,
                    extra: Default::default(),
                })
                .collect::<Vec<_>>();
            domain_from_field_references(
                scale_type,
                references.as_slice(),
                fields_reference.sort.as_ref(),
                config,
            )
        }
        ScaleDomainSpec::FieldsReferences(fields_references) => {
            domain_from_fields_references(scale_type, fields_references, config)
        }
        ScaleDomainSpec::FieldsSignals(fields_signals) => {
            let arrays = fields_signals
                .fields
                .iter()
                .map(|signal_expr| signal_expr_to_array(&signal_expr.signal, config))
                .collect::<Result<Vec<_>>>()?;
            merge_domain_arrays(scale_type, arrays, config)
        }
    }
}

fn domain_from_vec_strings(vec_strings: &ScaleVecStringsSpec) -> Result<ArrayRef> {
    let values = vec_strings
        .fields
        .iter()
        .map(|parts| match parts.len() {
            0 => ScalarValue::Null,
            1 => ScalarValue::from(parts[0].clone()),
            _ => ScalarValue::from(parts.join(".")),
        })
        .collect::<Vec<_>>();
    Ok(ScalarValue::iter_to_array(values)?)
}

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
                    range_from_named_alias(range_name, scale_type, domain_len)
                }
            } else if value.as_object().and_then(|obj| obj.get("step")).is_some() {
                Err(VegaFusionError::internal(format!(
                    "Scale {} has range.step but it could not be resolved",
                    scale.name
                )))
            } else if let Some(obj) = value.as_object() {
                if obj.get("scheme").is_some() {
                    range_from_scheme_object(obj, scale_type, domain_len, config)
                } else {
                    Err(VegaFusionError::internal(format!(
                        "Unsupported range object in this phase for scale {}: {value:?}",
                        scale.name
                    )))
                }
            } else {
                json_value_to_array(value)
            }
        }
    }
}

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

fn resolve_domain_raw_option(options: &HashMap<String, ScalarValue>) -> Result<Option<ArrayRef>> {
    let Some(domain_raw) = options.get("domain_raw").and_then(non_null_option_scalar) else {
        return Ok(None);
    };

    Ok(Some(scalar_to_array_or_singleton(domain_raw)?))
}

fn non_null_option_scalar(scalar: &ScalarValue) -> Option<&ScalarValue> {
    if scalar.is_null() {
        None
    } else {
        Some(scalar)
    }
}

fn option_f64(options: &HashMap<String, ScalarValue>, key: &str, default: f64) -> Result<f64> {
    if let Some(value) = options.get(key).and_then(non_null_option_scalar) {
        scalar_to_f64(value)
    } else {
        Ok(default)
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DomainSortOrder {
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
enum DomainSortMode {
    None,
    Key {
        order: DomainSortOrder,
    },
    Count {
        order: DomainSortOrder,
    },
    Aggregate {
        op: AggregateOpSpec,
        field: String,
        order: DomainSortOrder,
    },
}

#[derive(Clone)]
struct DomainEntry {
    key: ScalarValue,
    first_seen: usize,
    count: usize,
    metric: ScalarValue,
}

#[derive(Clone)]
struct DiscreteDomainSource {
    key_array: ArrayRef,
    sort_array: Option<ArrayRef>,
}

fn parse_domain_sort(
    sort: Option<&ScaleDataReferenceSort>,
    multidomain: bool,
) -> Result<DomainSortMode> {
    let Some(sort) = sort else {
        return Ok(DomainSortMode::None);
    };

    match sort {
        ScaleDataReferenceSort::Bool(false) => Ok(DomainSortMode::None),
        ScaleDataReferenceSort::Bool(true) => Ok(DomainSortMode::Key {
            order: DomainSortOrder::Ascending,
        }),
        ScaleDataReferenceSort::Parameters(params) => {
            parse_domain_sort_parameters(params, multidomain)
        }
    }
}

fn parse_domain_sort_parameters(
    params: &ScaleDataReferenceSortParameters,
    multidomain: bool,
) -> Result<DomainSortMode> {
    let order = sort_order_or_default(params.order.as_ref());
    let op = params.op.clone();
    let field = params.field.clone();

    if op.is_none() && field.is_none() {
        return Ok(DomainSortMode::Key { order });
    }

    if field.is_none() {
        if op != Some(AggregateOpSpec::Count) {
            return Err(VegaFusionError::internal(format!(
                "No field provided for sort aggregate op: {}",
                op.map(|o| o.name())
                    .unwrap_or_else(|| "unknown".to_string())
            )));
        }
        return Ok(DomainSortMode::Count { order });
    }

    let field = field.unwrap();

    if multidomain
        && op.as_ref().is_some_and(|op| {
            !matches!(
                op,
                AggregateOpSpec::Min | AggregateOpSpec::Max | AggregateOpSpec::Count
            )
        })
    {
        return Err(VegaFusionError::internal(format!(
            "Multiple domain scales can not be sorted using {}",
            op.map(|o| o.name())
                .unwrap_or_else(|| "unknown".to_string())
        )));
    }

    match op {
        Some(op) => Ok(DomainSortMode::Aggregate { op, field, order }),
        None if field == "key" => Ok(DomainSortMode::Key { order }),
        None if field == "count" => Ok(DomainSortMode::Count { order }),
        // Match Vega parser/runtime behavior:
        // non-key/non-count sort fields without an aggregate op are accepted
        // but do not produce a meaningful sort metric, which falls back to
        // stable first-seen ordering.
        None => Ok(DomainSortMode::None),
    }
}

fn sort_order_or_default(order: Option<&SortOrderSpec>) -> DomainSortOrder {
    match order {
        Some(SortOrderSpec::Descending) => DomainSortOrder::Descending,
        _ => DomainSortOrder::Ascending,
    }
}

fn domain_from_field_reference(
    scale_type: &ScaleTypeSpec,
    reference: &ScaleFieldReferenceSpec,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let key_array = lookup_data_column(config, reference.data.as_str(), reference.field.as_str())?;
    if !is_discrete_scale(scale_type) {
        return merge_domain_arrays(scale_type, vec![key_array], config);
    }

    let sort_mode = parse_domain_sort(reference.sort.as_ref(), false)?;
    let sort_array = match &sort_mode {
        DomainSortMode::Aggregate {
            op: AggregateOpSpec::Count,
            ..
        } => None,
        DomainSortMode::Aggregate { field, .. } => Some(lookup_data_column(
            config,
            reference.data.as_str(),
            field.as_str(),
        )?),
        _ => None,
    };

    build_discrete_domain_single(key_array, sort_array, &sort_mode)
}

fn domain_from_field_references(
    scale_type: &ScaleTypeSpec,
    references: &[ScaleFieldReferenceSpec],
    sort: Option<&ScaleDataReferenceSort>,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let key_arrays = references
        .iter()
        .map(|r| lookup_data_column(config, r.data.as_str(), r.field.as_str()))
        .collect::<Result<Vec<_>>>()?;

    if !is_discrete_scale(scale_type) {
        return merge_domain_arrays(scale_type, key_arrays, config);
    }

    let sort_mode = parse_domain_sort(sort, true)?;
    let mut sources = Vec::with_capacity(references.len());
    for (reference, key_array) in references.iter().zip(key_arrays) {
        let sort_array = match &sort_mode {
            DomainSortMode::Aggregate {
                op: AggregateOpSpec::Count,
                ..
            } => None,
            DomainSortMode::Aggregate { field, .. } => Some(lookup_data_column(
                config,
                reference.data.as_str(),
                field.as_str(),
            )?),
            _ => None,
        };
        sources.push(DiscreteDomainSource {
            key_array,
            sort_array,
        });
    }

    build_discrete_domain_multi(sources.as_slice(), &sort_mode)
}

fn domain_from_fields_references(
    scale_type: &ScaleTypeSpec,
    fields_references: &vegafusion_core::spec::scale::ScaleFieldsReferencesSpec,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    if !is_discrete_scale(scale_type) {
        let mut arrays = Vec::new();
        for reference_or_signal in &fields_references.fields {
            match reference_or_signal {
                ScaleDataReferenceOrSignalSpec::Reference(reference) => arrays.push(
                    lookup_data_column(config, reference.data.as_str(), reference.field.as_str())?,
                ),
                ScaleDataReferenceOrSignalSpec::Signal(signal_expr) => {
                    arrays.push(signal_expr_to_array(&signal_expr.signal, config)?)
                }
            }
        }
        return merge_domain_arrays(scale_type, arrays, config);
    }

    let sort_mode = parse_domain_sort(fields_references.sort.as_ref(), true)?;
    let mut sources = Vec::with_capacity(fields_references.fields.len());
    for reference_or_signal in &fields_references.fields {
        match reference_or_signal {
            ScaleDataReferenceOrSignalSpec::Reference(reference) => {
                let key_array =
                    lookup_data_column(config, reference.data.as_str(), reference.field.as_str())?;
                let sort_array = match &sort_mode {
                    DomainSortMode::Aggregate {
                        op: AggregateOpSpec::Count,
                        ..
                    } => None,
                    DomainSortMode::Aggregate { field, .. } => Some(lookup_data_column(
                        config,
                        reference.data.as_str(),
                        field.as_str(),
                    )?),
                    _ => None,
                };
                sources.push(DiscreteDomainSource {
                    key_array,
                    sort_array,
                });
            }
            ScaleDataReferenceOrSignalSpec::Signal(signal_expr) => {
                let key_array = signal_expr_to_array(&signal_expr.signal, config)?;
                let sort_array = match &sort_mode {
                    DomainSortMode::Aggregate {
                        op: AggregateOpSpec::Count,
                        ..
                    } => None,
                    DomainSortMode::Aggregate { field, .. } if field == "data" => {
                        Some(key_array.clone())
                    }
                    DomainSortMode::Aggregate { .. } => {
                        Some(new_null_array(&DataType::Null, key_array.len()))
                    }
                    _ => None,
                };
                sources.push(DiscreteDomainSource {
                    key_array,
                    sort_array,
                });
            }
        }
    }

    build_discrete_domain_multi(sources.as_slice(), &sort_mode)
}

fn build_discrete_domain_single(
    key_array: ArrayRef,
    sort_array: Option<ArrayRef>,
    sort_mode: &DomainSortMode,
) -> Result<ArrayRef> {
    let mut entries = collect_domain_entries(&key_array)?;

    match sort_mode {
        DomainSortMode::None | DomainSortMode::Key { .. } | DomainSortMode::Count { .. } => {}
        DomainSortMode::Aggregate {
            op: AggregateOpSpec::Count,
            ..
        } => {
            for entry in &mut entries {
                entry.metric = ScalarValue::Float64(Some(entry.count as f64));
            }
        }
        DomainSortMode::Aggregate { op, .. } => {
            let sort_array = sort_array.ok_or_else(|| {
                VegaFusionError::internal("Missing aggregate sort field array for scale domain")
            })?;
            let mut grouped: HashMap<ScalarValue, Vec<ScalarValue>> = HashMap::new();
            for i in 0..key_array.len() {
                let key = ScalarValue::try_from_array(key_array.as_ref(), i)?;
                let sort_value = ScalarValue::try_from_array(sort_array.as_ref(), i)?;
                grouped.entry(key).or_default().push(sort_value);
            }

            for entry in &mut entries {
                let values = grouped.get(&entry.key).cloned().unwrap_or_default();
                entry.metric = aggregate_sort_metric(op, values.as_slice())?;
            }
        }
    }

    sort_domain_entries(entries.as_mut_slice(), sort_mode);
    domain_entries_to_array(entries)
}

fn build_discrete_domain_multi(
    sources: &[DiscreteDomainSource],
    sort_mode: &DomainSortMode,
) -> Result<ArrayRef> {
    let mut entries = collect_domain_entries_multi(sources)?;
    match sort_mode {
        DomainSortMode::None | DomainSortMode::Key { .. } | DomainSortMode::Count { .. } => {}
        DomainSortMode::Aggregate {
            op: AggregateOpSpec::Count,
            ..
        } => {
            for entry in &mut entries {
                entry.metric = ScalarValue::Float64(Some(entry.count as f64));
            }
        }
        DomainSortMode::Aggregate { op, .. }
            if !matches!(op, AggregateOpSpec::Min | AggregateOpSpec::Max) =>
        {
            return Err(VegaFusionError::internal(format!(
                "Multiple domain scale sorting does not support aggregate op {} in this phase",
                op.name()
            )));
        }
        DomainSortMode::Aggregate { op, .. } => {
            let combine_min = matches!(op, AggregateOpSpec::Min);
            let mut combined_metrics: HashMap<ScalarValue, ScalarValue> = HashMap::new();

            for source in sources {
                let sort_array = source.sort_array.as_ref().ok_or_else(|| {
                    VegaFusionError::internal(
                        "Missing aggregate sort field array for multi-domain scale",
                    )
                })?;
                let mut grouped: HashMap<ScalarValue, Vec<ScalarValue>> = HashMap::new();
                for i in 0..source.key_array.len() {
                    let key = ScalarValue::try_from_array(source.key_array.as_ref(), i)?;
                    let sort_value = ScalarValue::try_from_array(sort_array.as_ref(), i)?;
                    grouped.entry(key).or_default().push(sort_value);
                }

                for (key, values) in grouped {
                    let metric = aggregate_sort_metric(op, values.as_slice())?;
                    combined_metrics
                        .entry(key)
                        .and_modify(|existing| {
                            let ord = vega_ascending_scalar(metric.clone(), existing.clone());
                            if (combine_min && ord == Ordering::Less)
                                || (!combine_min && ord == Ordering::Greater)
                            {
                                *existing = metric.clone();
                            }
                        })
                        .or_insert(metric);
                }
            }

            for entry in &mut entries {
                if let Some(metric) = combined_metrics.get(&entry.key) {
                    entry.metric = metric.clone();
                }
            }
        }
    }

    sort_domain_entries(entries.as_mut_slice(), sort_mode);
    domain_entries_to_array(entries)
}

fn collect_domain_entries(key_array: &ArrayRef) -> Result<Vec<DomainEntry>> {
    let mut entries = Vec::<DomainEntry>::new();
    let mut entry_indices = HashMap::<ScalarValue, usize>::new();
    for i in 0..key_array.len() {
        let key = ScalarValue::try_from_array(key_array.as_ref(), i)?;
        let idx = *entry_indices.entry(key.clone()).or_insert_with(|| {
            let idx = entries.len();
            entries.push(DomainEntry {
                key,
                first_seen: i,
                count: 0,
                metric: ScalarValue::Null,
            });
            idx
        });
        entries[idx].count += 1;
    }
    Ok(entries)
}

fn collect_domain_entries_multi(sources: &[DiscreteDomainSource]) -> Result<Vec<DomainEntry>> {
    let mut entries = Vec::<DomainEntry>::new();
    let mut entry_indices = HashMap::<ScalarValue, usize>::new();
    let mut seen_index = 0usize;
    for source in sources {
        for i in 0..source.key_array.len() {
            let key = ScalarValue::try_from_array(source.key_array.as_ref(), i)?;
            let idx = *entry_indices.entry(key.clone()).or_insert_with(|| {
                let idx = entries.len();
                entries.push(DomainEntry {
                    key,
                    first_seen: seen_index,
                    count: 0,
                    metric: ScalarValue::Null,
                });
                seen_index += 1;
                idx
            });
            entries[idx].count += 1;
        }
    }
    Ok(entries)
}

fn sort_domain_entries(entries: &mut [DomainEntry], sort_mode: &DomainSortMode) {
    entries.sort_by(|a, b| {
        let cmp = match sort_mode {
            DomainSortMode::None => Ordering::Equal,
            DomainSortMode::Key { order } => {
                apply_sort_order(vega_ascending_scalar(a.key.clone(), b.key.clone()), *order)
            }
            DomainSortMode::Count { order } => {
                apply_sort_order((a.count as f64).total_cmp(&(b.count as f64)), *order)
            }
            DomainSortMode::Aggregate { order, .. } => apply_sort_order(
                vega_ascending_scalar(a.metric.clone(), b.metric.clone()),
                *order,
            ),
        };
        if cmp == Ordering::Equal {
            a.first_seen.cmp(&b.first_seen)
        } else {
            cmp
        }
    });
}

fn apply_sort_order(ordering: Ordering, order: DomainSortOrder) -> Ordering {
    match order {
        DomainSortOrder::Ascending => ordering,
        DomainSortOrder::Descending => ordering.reverse(),
    }
}

fn domain_entries_to_array(entries: Vec<DomainEntry>) -> Result<ArrayRef> {
    if entries.is_empty() {
        Ok(new_empty_array(&DataType::Utf8))
    } else {
        Ok(ScalarValue::iter_to_array(
            entries.into_iter().map(|e| e.key).collect::<Vec<_>>(),
        )?)
    }
}

fn vega_ascending_scalar(a: ScalarValue, b: ScalarValue) -> Ordering {
    if a.is_null() && !b.is_null() {
        return Ordering::Less;
    }
    if !a.is_null() && b.is_null() {
        return Ordering::Greater;
    }
    if a.is_null() && b.is_null() {
        return Ordering::Equal;
    }

    if let (Some(af), Some(bf)) = (scalar_as_f64(&a), scalar_as_f64(&b)) {
        return if af.is_nan() && !bf.is_nan() {
            Ordering::Less
        } else if !af.is_nan() && bf.is_nan() {
            Ordering::Greater
        } else {
            af.total_cmp(&bf)
        };
    }

    match a.partial_cmp(&b) {
        Some(ord) => ord,
        None => a.to_string().cmp(&b.to_string()),
    }
}

fn scalar_as_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

fn aggregate_sort_metric(op: &AggregateOpSpec, values: &[ScalarValue]) -> Result<ScalarValue> {
    use AggregateOpSpec::*;

    let non_null = values
        .iter()
        .filter(|v| !v.is_null())
        .cloned()
        .collect::<Vec<_>>();

    match op {
        Count => Ok(ScalarValue::Float64(Some(values.len() as f64))),
        Valid => Ok(ScalarValue::Float64(Some(non_null.len() as f64))),
        Missing => Ok(ScalarValue::Float64(Some(
            (values.len() - non_null.len()) as f64,
        ))),
        Distinct => Ok(ScalarValue::Float64(Some(
            non_null.into_iter().collect::<HashSet<_>>().len() as f64,
        ))),
        Min => Ok(non_null
            .into_iter()
            .min_by(|a, b| vega_ascending_scalar(a.clone(), b.clone()))
            .unwrap_or(ScalarValue::Null)),
        Max => Ok(non_null
            .into_iter()
            .max_by(|a, b| vega_ascending_scalar(a.clone(), b.clone()))
            .unwrap_or(ScalarValue::Null)),
        Sum => {
            let nums = numeric_values(values);
            Ok(ScalarValue::Float64(Some(nums.into_iter().sum())))
        }
        Product => {
            let nums = numeric_values(values);
            Ok(ScalarValue::Float64(Some(nums.into_iter().product())))
        }
        Mean | Average => {
            let nums = numeric_values(values);
            if nums.is_empty() {
                Ok(ScalarValue::Null)
            } else {
                let sum: f64 = nums.iter().sum();
                Ok(ScalarValue::Float64(Some(sum / (nums.len() as f64))))
            }
        }
        Variance | Variancep | Stdev | Stdevp | Stderr => {
            let nums = numeric_values(values);
            if nums.is_empty() {
                return Ok(ScalarValue::Null);
            }

            let n = nums.len() as f64;
            let mean = nums.iter().sum::<f64>() / n;
            let sum_sq = nums.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>();
            let variance = match op {
                Variance | Stdev | Stderr => {
                    if nums.len() <= 1 {
                        return Ok(ScalarValue::Null);
                    }
                    sum_sq / (n - 1.0)
                }
                Variancep | Stdevp => sum_sq / n,
                _ => unreachable!(),
            };

            let value = match op {
                Variance | Variancep => variance,
                Stdev | Stdevp => variance.sqrt(),
                Stderr => variance.sqrt() / n.sqrt(),
                _ => unreachable!(),
            };
            Ok(ScalarValue::Float64(Some(value)))
        }
        Median | Q1 | Q3 => {
            let mut nums = numeric_values(values);
            nums.sort_by(|a, b| a.total_cmp(b));
            if nums.is_empty() {
                return Ok(ScalarValue::Null);
            }
            let q = match op {
                Median => 0.5,
                Q1 => 0.25,
                Q3 => 0.75,
                _ => unreachable!(),
            };
            Ok(ScalarValue::Float64(quantile(&nums, q)))
        }
        unsupported => Err(VegaFusionError::internal(format!(
            "Unsupported domain sort aggregate op in this phase: {}",
            unsupported.name()
        ))),
    }
}

fn numeric_values(values: &[ScalarValue]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|v| if v.is_null() { None } else { scalar_as_f64(v) })
        .collect()
}

fn quantile(values: &[f64], q: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() == 1 {
        return Some(values[0]);
    }
    let p = q.clamp(0.0, 1.0) * ((values.len() - 1) as f64);
    let lo = p.floor() as usize;
    let hi = p.ceil() as usize;
    if lo == hi {
        Some(values[lo])
    } else {
        let w = p - (lo as f64);
        Some(values[lo] + (values[hi] - values[lo]) * w)
    }
}

fn merge_domain_arrays(
    scale_type: &ScaleTypeSpec,
    arrays: Vec<ArrayRef>,
    _config: &CompilationConfig,
) -> Result<ArrayRef> {
    if arrays.is_empty() {
        return Err(VegaFusionError::internal(
            "Scale domain references resolved to an empty set of arrays",
        ));
    }

    if is_continuous_scale(scale_type) {
        if !matches!(scale_type, ScaleTypeSpec::Time | ScaleTypeSpec::Utc) {
            let mut min_num: Option<f64> = None;
            let mut max_num: Option<f64> = None;

            for arr in arrays {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let value = ScalarValue::try_from_array(arr.as_ref(), i)?;
                    let Ok(num) = scalar_to_f64(&value) else {
                        continue;
                    };
                    if !num.is_finite() {
                        continue;
                    }
                    if min_num.map(|m| num < m).unwrap_or(true) {
                        min_num = Some(num);
                    }
                    if max_num.map(|m| num > m).unwrap_or(true) {
                        max_num = Some(num);
                    }
                }
            }

            return match (min_num, max_num) {
                (Some(min_num), Some(max_num)) => Ok(ScalarValue::iter_to_array(vec![
                    ScalarValue::Float64(Some(min_num)),
                    ScalarValue::Float64(Some(max_num)),
                ])?),
                _ => Ok(new_empty_array(&DataType::Float64)),
            };
        }

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

fn is_discrete_scale(scale_type: &ScaleTypeSpec) -> bool {
    matches!(
        scale_type,
        ScaleTypeSpec::Ordinal | ScaleTypeSpec::Band | ScaleTypeSpec::Point
    )
}

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

fn eval_signal_expr(expr_str: &str, config: &CompilationConfig) -> Result<ScalarValue> {
    let expression = parse(expr_str)?;
    let compiled = compile(&expression, config, None)?;
    compiled.eval_to_scalar()
}

fn signal_expr_to_array(expr_str: &str, config: &CompilationConfig) -> Result<ArrayRef> {
    let scalar = eval_signal_expr(expr_str, config)?;
    scalar_to_array_or_singleton(&scalar)
}

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

fn range_from_named_alias(
    range_name: &str,
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
) -> Result<ArrayRef> {
    let Some(alias) = default_named_range(range_name) else {
        return Err(VegaFusionError::internal(format!(
            "Unsupported named range {range_name:?} in this phase"
        )));
    };

    match alias {
        NamedRange::Values(values) => values_to_string_array(values),
        NamedRange::Scheme { scheme, extent } => {
            range_from_scheme_name(scheme, extent, None, scale_type, domain_len)
        }
    }
}

fn range_from_scheme_object(
    obj: &serde_json::Map<String, serde_json::Value>,
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
    config: &CompilationConfig,
) -> Result<ArrayRef> {
    let scheme = obj
        .get("scheme")
        .ok_or_else(|| VegaFusionError::internal("Range scheme object missing `scheme` key"))?;
    let scheme_input = resolve_scheme_input(scheme, config)?;
    let extent = resolve_scheme_extent(obj.get("extent"), config)?;
    let count = resolve_scheme_count(obj.get("count"), config)?;

    match scheme_input {
        SchemeInput::Name(name) => {
            range_from_scheme_name(name.as_str(), extent, count, scale_type, domain_len)
        }
        SchemeInput::Values(values) => {
            range_from_values(values, false, extent, count, scale_type, domain_len)
        }
    }
}

enum SchemeInput {
    Name(String),
    Values(Vec<String>),
}

fn resolve_scheme_input(
    scheme: &serde_json::Value,
    config: &CompilationConfig,
) -> Result<SchemeInput> {
    if let Some(name) = scheme.as_str() {
        return Ok(SchemeInput::Name(name.to_string()));
    }

    if let Some(values) = scheme.as_array() {
        return Ok(SchemeInput::Values(json_values_to_strings(values)?));
    }

    let scalar = eval_json_or_signal_scalar(scheme, config)?;
    if let Some(name) = scalar_to_string(&scalar) {
        return Ok(SchemeInput::Name(name));
    }

    let arr = scalar_to_array_or_singleton(&scalar)?;
    let mut values = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let val = ScalarValue::try_from_array(arr.as_ref(), i)?;
        let Some(s) = scalar_to_string(&val) else {
            return Err(VegaFusionError::internal(format!(
                "Scheme values must be strings, received {val:?}"
            )));
        };
        values.push(s);
    }
    Ok(SchemeInput::Values(values))
}

fn resolve_scheme_extent(
    extent: Option<&serde_json::Value>,
    config: &CompilationConfig,
) -> Result<Option<(f64, f64)>> {
    let Some(extent) = extent else {
        return Ok(None);
    };

    let scalar = eval_json_or_signal_scalar(extent, config)?;
    let arr = scalar_to_array_or_singleton(&scalar)?;
    if arr.len() < 2 {
        return Err(VegaFusionError::internal(format!(
            "Scheme extent must contain two numeric values, received length {}",
            arr.len()
        )));
    }
    let lo = ScalarValue::try_from_array(arr.as_ref(), 0)?;
    let hi = ScalarValue::try_from_array(arr.as_ref(), 1)?;
    Ok(Some((scalar_to_f64(&lo)?, scalar_to_f64(&hi)?)))
}

fn resolve_scheme_count(
    count: Option<&serde_json::Value>,
    config: &CompilationConfig,
) -> Result<Option<usize>> {
    let Some(count) = count else {
        return Ok(None);
    };
    let scalar = eval_json_or_signal_scalar(count, config)?;
    let count = scalar_to_f64(&scalar)?;
    if count <= 0.0 {
        return Ok(Some(1));
    }
    Ok(Some(count.round() as usize))
}

fn range_from_scheme_name(
    name: &str,
    extent: Option<(f64, f64)>,
    count: Option<usize>,
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
) -> Result<ArrayRef> {
    let Some(scheme) = lookup_scheme(name) else {
        return Err(VegaFusionError::internal(format!(
            "Unrecognized scheme name: {name}"
        )));
    };

    match scheme {
        SchemePalette::Discrete(values) => {
            let values = values.iter().map(|v| (*v).to_string()).collect::<Vec<_>>();
            range_from_values(values, false, extent, count, scale_type, domain_len)
        }
        SchemePalette::Continuous(hex) => {
            let values = decode_continuous_scheme(hex);
            range_from_values(values, true, extent, count, scale_type, domain_len)
        }
    }
}

fn range_from_values(
    values: Vec<String>,
    continuous_source: bool,
    extent: Option<(f64, f64)>,
    count: Option<usize>,
    scale_type: &ScaleTypeSpec,
    domain_len: usize,
) -> Result<ArrayRef> {
    if values.is_empty() {
        return Err(VegaFusionError::internal(
            "Scheme values resolved to an empty range",
        ));
    }

    let needs_sampling =
        extent.is_some() || count.is_some() || (continuous_source && is_discrete_scale(scale_type));
    let sampled = if needs_sampling {
        let sampled_count = count.or_else(|| {
            if continuous_source && is_discrete_scale(scale_type) {
                Some(domain_len.max(1))
            } else {
                None
            }
        });
        sample_colors(
            values.as_slice(),
            sampled_count,
            extent,
            continuous_source && is_discrete_scale(scale_type),
        )?
    } else {
        values
    };

    let scalars = sampled
        .into_iter()
        .map(ScalarValue::from)
        .collect::<Vec<_>>();
    Ok(ScalarValue::iter_to_array(scalars)?)
}

fn sample_colors(
    values: &[String],
    count: Option<usize>,
    extent: Option<(f64, f64)>,
    quantize_interior: bool,
) -> Result<Vec<String>> {
    let mut lo = extent.map(|e| e.0).unwrap_or(0.0);
    let mut hi = extent.map(|e| e.1).unwrap_or(1.0);
    lo = lo.clamp(0.0, 1.0);
    hi = hi.clamp(0.0, 1.0);

    let target = count.unwrap_or(values.len().max(2));
    let rgbs = values
        .iter()
        .map(|v| parse_hex_color(v))
        .collect::<Result<Vec<_>>>()?;
    if rgbs.len() == 1 {
        return Ok(vec![rgb_to_hex(rgbs[0]); target]);
    }

    if target == 1 {
        let t = (lo + hi) / 2.0;
        return Ok(vec![interpolate_color(rgbs.as_slice(), t)]);
    }

    let mut out = Vec::with_capacity(target);
    for i in 0..target {
        let frac = if target <= 1 {
            0.0
        } else if quantize_interior {
            (i as f64 + 1.0) / (target as f64 + 1.0)
        } else {
            (i as f64) / ((target - 1) as f64)
        };
        let t = lo + (hi - lo) * frac;
        out.push(interpolate_color(rgbs.as_slice(), t));
    }

    Ok(out)
}

fn parse_hex_color(value: &str) -> Result<[f64; 3]> {
    let hex = value.trim_start_matches('#');
    if hex.len() != 6 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(VegaFusionError::internal(format!(
            "Only 6-digit hex colors are supported for scheme sampling, received {value:?}"
        )));
    }

    let parse_channel = |range: RangeInclusive<usize>| -> Result<f64> {
        let slice = &hex[range];
        let parsed = u8::from_str_radix(slice, 16).map_err(|err| {
            VegaFusionError::internal(format!(
                "Failed to parse hex color component {slice:?}: {err}"
            ))
        })?;
        Ok((parsed as f64) / 255.0)
    };

    Ok([
        parse_channel(0..=1)?,
        parse_channel(2..=3)?,
        parse_channel(4..=5)?,
    ])
}

fn interpolate_color(colors: &[[f64; 3]], t: f64) -> String {
    let n = colors.len();
    if n == 1 {
        return rgb_to_hex(colors[0]);
    }

    let clamped = t.clamp(0.0, 1.0);
    let scaled = clamped * ((n - 1) as f64);
    let lo = scaled.floor() as usize;
    let hi = scaled.ceil() as usize;
    if lo == hi {
        return rgb_to_hex(colors[lo]);
    }

    let f = scaled - (lo as f64);
    let c0 = colors[lo];
    let c1 = colors[hi];
    let rgb = [
        c0[0] + (c1[0] - c0[0]) * f,
        c0[1] + (c1[1] - c0[1]) * f,
        c0[2] + (c1[2] - c0[2]) * f,
    ];
    rgb_to_hex(rgb)
}

fn rgb_to_hex(rgb: [f64; 3]) -> String {
    format!(
        "#{:02x}{:02x}{:02x}",
        (rgb[0].clamp(0.0, 1.0) * 255.0).round() as u8,
        (rgb[1].clamp(0.0, 1.0) * 255.0).round() as u8,
        (rgb[2].clamp(0.0, 1.0) * 255.0).round() as u8
    )
}

fn values_to_string_array(values: &[&str]) -> Result<ArrayRef> {
    let scalars = values
        .iter()
        .map(|v| ScalarValue::from((*v).to_string()))
        .collect::<Vec<_>>();
    Ok(ScalarValue::iter_to_array(scalars)?)
}

fn json_values_to_strings(values: &[serde_json::Value]) -> Result<Vec<String>> {
    values
        .iter()
        .map(|v| {
            v.as_str().map(|s| s.to_string()).ok_or_else(|| {
                VegaFusionError::internal(format!(
                    "Expected string value in scheme array, received {v:?}"
                ))
            })
        })
        .collect()
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(v)) => Some(v.clone()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Utf8View(Some(v)) => Some(v.clone()),
        _ => None,
    }
}

fn apply_reverse_option(
    range: ArrayRef,
    options: &mut HashMap<String, ScalarValue>,
) -> Result<ArrayRef> {
    let reverse = options
        .get("reverse")
        .and_then(non_null_option_scalar)
        .map(scalar_to_bool)
        .transpose()?
        .unwrap_or(false);
    options.remove("reverse");

    if !reverse || range.len() <= 1 {
        return Ok(range);
    }

    let mut reversed = Vec::with_capacity(range.len());
    for idx in (0..range.len()).rev() {
        reversed.push(ScalarValue::try_from_array(range.as_ref(), idx)?);
    }
    Ok(ScalarValue::iter_to_array(reversed)?)
}

fn consume_range_construction_options(options: &mut HashMap<String, ScalarValue>) {
    // Vega parser/runtime uses these to construct ranges but they are not avenger options.
    for key in ["interpolate", "interpolate_gamma", "domain_implicit"] {
        options.remove(key);
    }
}

fn scalar_to_bool(value: &ScalarValue) -> Result<bool> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        _ => Err(VegaFusionError::internal(format!(
            "Expected boolean scalar value, received {value:?}"
        ))),
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Result<f64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;
    use serde_json::json;
    use vegafusion_common::arrow::array::{BooleanArray, TimestampNanosecondArray, UInt64Array};
    use vegafusion_common::arrow::datatypes::Int64Type;
    use vegafusion_common::data::table::VegaFusionTable;

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

    fn config_with_data(name: &str, values: serde_json::Value) -> CompilationConfig {
        let mut config = CompilationConfig::default();
        config.data_scope.insert(
            name.to_string(),
            VegaFusionTable::from_json(&values).unwrap(),
        );
        config
    }

    fn scalar_strings(values: &ArrayRef) -> Vec<String> {
        (0..values.len())
            .map(|i| scalar_string(values, i))
            .collect()
    }

    fn scalar_text(values: &ArrayRef, index: usize) -> String {
        let value = ScalarValue::try_from_array(values, index).unwrap();
        match value {
            ScalarValue::Null => "null".to_string(),
            ScalarValue::Utf8(Some(v)) => v,
            ScalarValue::LargeUtf8(Some(v)) => v,
            ScalarValue::Utf8View(Some(v)) => v,
            ScalarValue::Int64(Some(v)) => v.to_string(),
            ScalarValue::Int32(Some(v)) => v.to_string(),
            ScalarValue::Float64(Some(v)) => v.to_string(),
            other => format!("{other:?}"),
        }
    }

    fn scalar_texts(values: &ArrayRef) -> Vec<String> {
        (0..values.len()).map(|i| scalar_text(values, i)).collect()
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

    fn scalar_string(values: &ArrayRef, index: usize) -> String {
        let value = ScalarValue::try_from_array(values, index).unwrap();
        match value {
            ScalarValue::Utf8(Some(v)) => v,
            ScalarValue::LargeUtf8(Some(v)) => v,
            ScalarValue::Utf8View(Some(v)) => v,
            _ => panic!("Expected string scalar, received {value:?}"),
        }
    }

    #[test]
    fn test_explicit_domain_does_not_apply_default_zero_for_linear_pow_sqrt() {
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
            assert_eq!(lo, 2.0, "scale type {scale_type}");
            assert_eq!(hi, 5.0, "scale type {scale_type}");
            assert!(!state.options.contains_key("zero"));
        }
    }

    #[test]
    fn test_data_domain_applies_default_zero_for_linear_pow_sqrt() {
        let config = config_with_data("tbl", json!([{"v": 2.0}, {"v": 5.0}]));
        for scale_type in ["linear", "pow", "sqrt"] {
            let scale: ScaleSpec = serde_json::from_value(json!({
                "name": "x",
                "type": scale_type,
                "domain": {"data": "tbl", "field": "v"},
                "range": [0, 100]
            }))
            .unwrap();
            let state = resolve_scale_state(&scale, &config).unwrap();
            let (lo, hi) = scalar_f64(&state.domain);
            assert_eq!(lo, 0.0, "scale type {scale_type}");
            assert_eq!(hi, 5.0, "scale type {scale_type}");
            assert!(!state.options.contains_key("zero"));
        }
    }

    #[test]
    fn test_numeric_continuous_domain_coerces_string_values() {
        let config = config_with_data(
            "tbl",
            json!([
                {"v": "12.8"},
                {"v": "10.6"},
                {"v": "8.9"},
                {"v": "39.0"}
            ]),
        );

        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "y",
            "type": "linear",
            "domain": {"data": "tbl", "field": "v"},
            "range": [300, 0],
            "zero": false
        }))
        .unwrap();

        let state = resolve_scale_state(&scale, &config).unwrap();
        let (lo, hi) = scalar_f64(&state.domain);
        assert_eq!(lo, 8.9);
        assert_eq!(hi, 39.0);
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
    fn test_discrete_domain_sort_true_orders_keys_ascending() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {"data": "t", "field": "k", "sort": true},
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b"},
                {"k": "a"},
                {"k": "c"},
                {"k": "a"}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(scalar_strings(&state.domain), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_discrete_domain_sort_false_preserves_first_seen_order() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {"data": "t", "field": "k", "sort": false},
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b"},
                {"k": "a"},
                {"k": "c"},
                {"k": "a"}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(scalar_strings(&state.domain), vec!["b", "a", "c"]);
    }

    #[test]
    fn test_discrete_domain_sort_aggregate_min_descending() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {
                "data": "t",
                "field": "k",
                "sort": {"op": "min", "field": "sort_index", "order": "descending"}
            },
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "A", "sort_index": 10},
                {"k": "A", "sort_index":  5},
                {"k": "B", "sort_index": 20},
                {"k": "C", "sort_index": 15}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(scalar_strings(&state.domain), vec!["B", "C", "A"]);
    }

    #[test]
    fn test_discrete_domain_sort_field_without_op_is_noop_like_vega() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {
                "data": "t",
                "field": "k",
                "sort": {"field": "other", "order": "descending"}
            },
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b", "other": 2},
                {"k": "a", "other": 1},
                {"k": "c", "other": 3}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(scalar_strings(&state.domain), vec!["b", "a", "c"]);
    }

    #[test]
    fn test_discrete_domain_sort_field_key_descending() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {
                "data": "t",
                "field": "k",
                "sort": {"field": "key", "order": "descending"}
            },
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b"},
                {"k": "a"},
                {"k": "c"}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(scalar_strings(&state.domain), vec!["c", "b", "a"]);
    }

    #[test]
    fn test_multidomain_sort_field_without_op_is_noop_like_vega() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "ordinal",
            "domain": {
                "fields": [
                    {"data": "t", "field": "a"},
                    {"data": "t", "field": "b"}
                ],
                "sort": {"field": "other", "order": "descending"}
            },
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"a": "b", "b": "y", "other": 2},
                {"a": "a", "b": "x", "other": 1},
                {"a": "c", "b": "x", "other": 3},
                {"a": "a", "b": "z", "other": 4}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(
            scalar_strings(&state.domain),
            vec!["b", "a", "c", "y", "x", "z"]
        );
    }

    #[test]
    fn test_discrete_domain_sort_true_keeps_null_category() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {"data": "t", "field": "k", "sort": true},
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b"},
                {"k": null},
                {"k": "a"}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(
            scalar_texts(&state.domain),
            vec![
                DISCRETE_NULL_SENTINEL.to_string(),
                "a".to_string(),
                "b".to_string()
            ]
        );
    }

    #[test]
    fn test_discrete_domain_sort_false_keeps_null_category() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "x",
            "type": "band",
            "domain": {"data": "t", "field": "k", "sort": false},
            "range": [0, 100]
        }))
        .unwrap();
        let config = config_with_data(
            "t",
            json!([
                {"k": "b"},
                {"k": null},
                {"k": "a"}
            ]),
        );

        let state = resolve_scale_state(&scale, &config).unwrap();
        assert_eq!(
            scalar_texts(&state.domain),
            vec![
                "b".to_string(),
                DISCRETE_NULL_SENTINEL.to_string(),
                "a".to_string()
            ]
        );
    }

    #[test]
    fn test_discrete_temporal_domain_normalizes_to_int64() {
        let domain = Arc::new(
            TimestampNanosecondArray::from(vec![Some(1_000_000_000_i64), Some(2_000_000_000_i64)])
                .with_timezone("UTC"),
        ) as ArrayRef;

        let normalized = normalize_discrete_domain_for_avenger(&ScaleTypeSpec::Ordinal, domain)
            .expect("normalize temporal domain");
        assert_eq!(normalized.data_type(), &DataType::Int64);
        let values = normalized.as_primitive::<Int64Type>();
        assert_eq!(values.value(0), 1_000_000_000_i64);
        assert_eq!(values.value(1), 2_000_000_000_i64);
    }

    #[test]
    fn test_discrete_temporal_domain_with_null_uses_sentinel_path() {
        let domain = Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(1_000_000_000_i64),
                None,
                Some(2_000_000_000_i64),
            ])
            .with_timezone("UTC"),
        ) as ArrayRef;

        let normalized = normalize_discrete_domain_for_avenger(&ScaleTypeSpec::Band, domain)
            .expect("normalize temporal domain");
        assert!(matches!(
            normalized.data_type(),
            DataType::Timestamp(_, Some(_))
        ));

        let encoded =
            encode_discrete_null_domain(&ScaleTypeSpec::Band, normalized).expect("encode nulls");
        assert_eq!(encoded.data_type(), &DataType::Utf8);
        let texts = scalar_texts(&encoded);
        assert_eq!(texts.len(), 3);
        assert_eq!(texts[1], DISCRETE_NULL_SENTINEL);
        assert_ne!(texts[0], DISCRETE_NULL_SENTINEL);
        assert_ne!(texts[2], DISCRETE_NULL_SENTINEL);
    }

    #[test]
    fn test_discrete_unsigned_domain_normalizes_for_avenger() {
        let domain = Arc::new(UInt64Array::from(vec![1_u64, 2_u64, 3_u64])) as ArrayRef;
        let normalized = normalize_discrete_domain_for_avenger(&ScaleTypeSpec::Ordinal, domain)
            .expect("normalize unsigned domain");
        assert_eq!(normalized.data_type(), &DataType::Int64);
        assert_eq!(
            scalar_texts(&normalized),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn test_discrete_boolean_domain_normalizes_for_avenger() {
        let domain = Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef;
        let normalized = normalize_discrete_domain_for_avenger(&ScaleTypeSpec::Ordinal, domain)
            .expect("normalize boolean domain");
        assert_eq!(normalized.data_type(), &DataType::Utf8);
        assert_eq!(
            scalar_texts(&normalized),
            vec!["false".to_string(), "true".to_string()]
        );
    }

    #[test]
    fn test_named_ranges_category_and_heatmap() {
        let category: ScaleSpec = serde_json::from_value(json!({
            "name": "fill",
            "type": "ordinal",
            "domain": ["A", "B", "C"],
            "range": "category"
        }))
        .unwrap();

        let heatmap: ScaleSpec = serde_json::from_value(json!({
            "name": "fill",
            "type": "linear",
            "domain": [0, 1],
            "range": "heatmap"
        }))
        .unwrap();

        let category_state = resolve_scale_state(&category, &CompilationConfig::default()).unwrap();
        let heatmap_state = resolve_scale_state(&heatmap, &CompilationConfig::default()).unwrap();

        assert_eq!(category_state.range.len(), 10);
        assert_eq!(scalar_string(&category_state.range, 0), "#4c78a8");
        assert_eq!(heatmap_state.range.len(), 11);
        assert_eq!(scalar_string(&heatmap_state.range, 0), "#eff9bd");
    }

    #[test]
    fn test_named_ordinal_range_uses_vega_quantize_sampling() {
        let ordinal: ScaleSpec = serde_json::from_value(json!({
            "name": "stroke",
            "type": "ordinal",
            "domain": [3, 4, 5, 6, 8],
            "range": "ordinal"
        }))
        .unwrap();

        let state = resolve_scale_state(&ordinal, &CompilationConfig::default()).unwrap();
        assert_eq!(state.range.len(), 5);
        assert_eq!(
            scalar_texts(&state.range),
            vec![
                "#afd1e7".to_string(),
                "#86bcdc".to_string(),
                "#5ba3cf".to_string(),
                "#3887c0".to_string(),
                "#1b69ad".to_string()
            ]
        );
    }

    #[test]
    fn test_range_scheme_object_supported() {
        let scheme: ScaleSpec = serde_json::from_value(json!({
            "name": "fill",
            "type": "linear",
            "domain": [0, 1],
            "range": {"scheme": "inferno"}
        }))
        .unwrap();

        let state = resolve_scale_state(&scheme, &CompilationConfig::default()).unwrap();
        assert_eq!(state.range.len(), 31);
        assert_eq!(scalar_string(&state.range, 0), "#000004");
        assert_eq!(
            scalar_string(&state.range, state.range.len() - 1),
            "#fcffa4"
        );
    }

    #[test]
    fn test_range_scheme_category20_supported() {
        let scheme: ScaleSpec = serde_json::from_value(json!({
            "name": "fill",
            "type": "ordinal",
            "domain": ["a", "b", "c"],
            "range": {"scheme": "category20"}
        }))
        .unwrap();

        let state = resolve_scale_state(&scheme, &CompilationConfig::default()).unwrap();
        assert_eq!(state.range.len(), 20);
        assert_eq!(scalar_string(&state.range, 0), "#1f77b4");
        assert_eq!(
            scalar_string(&state.range, state.range.len() - 1),
            "#9edae5"
        );
    }

    #[test]
    fn test_domain_fields_vec_strings_supported() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "xOffset",
            "type": "band",
            "domain": {"fields": [["Worldwide Gross"], ["US Gross"]]},
            "range": {"step": 20}
        }))
        .unwrap();

        let state = resolve_scale_state(&scale, &CompilationConfig::default()).unwrap();
        assert_eq!(
            scalar_texts(&state.domain),
            vec!["Worldwide Gross".to_string(), "US Gross".to_string()]
        );
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
