use crate::error::Result;
use crate::expression::parser::parse;
use crate::proto::gen::tasks::Variable;
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::values::{SignalExpressionSpec, SortOrderSpec};
use crate::task_graph::task::InputVariable;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<ScaleTypeSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<ScaleDomainSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<ScaleRangeSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bins: Option<ScaleBinsSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl ScaleSpec {
    pub fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // domain
        if let Some(domain) = &self.domain {
            match domain {
                ScaleDomainSpec::FieldReference(reference) => {
                    vars.insert(InputVariable {
                        var: Variable::new_data(&reference.data),
                        propagate: true,
                    });
                }
                ScaleDomainSpec::FieldsReference(fields_reference) => {
                    if let Some(data) = &fields_reference.data {
                        vars.insert(InputVariable {
                            var: Variable::new_data(data),
                            propagate: true,
                        });
                    }
                }
                ScaleDomainSpec::FieldsReferences(fields_references) => {
                    for value in &fields_references.fields {
                        match value {
                            ScaleDataReferenceOrSignalSpec::Reference(reference) => {
                                vars.insert(InputVariable {
                                    var: Variable::new_data(&reference.data),
                                    propagate: true,
                                });
                            }
                            ScaleDataReferenceOrSignalSpec::Signal(signal_expr) => {
                                add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                            }
                        }
                    }
                }
                ScaleDomainSpec::FieldsSignals(fields_signals) => {
                    for signal_expr in &fields_signals.fields {
                        add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                    }
                }
                ScaleDomainSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                }
                ScaleDomainSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                        }
                    }
                }
                ScaleDomainSpec::Value(Value::Object(obj)) => {
                    add_domain_object_deps(obj, &mut vars)?;
                }
                _ => {}
            }
        }

        // range
        if let Some(range) = &self.range {
            match range {
                ScaleRangeSpec::Reference(reference) => {
                    vars.insert(InputVariable {
                        var: Variable::new_data(&reference.data),
                        propagate: true,
                    });
                }
                ScaleRangeSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                }
                ScaleRangeSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                        }
                    }
                }
                ScaleRangeSpec::Value(Value::String(s)) => {
                    if matches!(s.as_str(), "width" | "height") {
                        vars.insert(InputVariable {
                            var: Variable::new_signal(s),
                            propagate: true,
                        });
                    }
                }
                ScaleRangeSpec::Value(Value::Object(obj)) => {
                    if let Some(signal_expr) = obj
                        .get("step")
                        .and_then(|step| step.as_object())
                        .and_then(|step| step.get("signal"))
                        .and_then(|v| v.as_str())
                    {
                        add_signal_expr_deps(signal_expr, &mut vars)?;
                    }
                }
                _ => {}
            }
        }

        // bins
        if let Some(bins) = &self.bins {
            match bins {
                ScaleBinsSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                }
                ScaleBinsSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars)?;
                        }
                    }
                }
                ScaleBinsSpec::Value(Value::Object(obj)) => {
                    if let Some(signal_expr) = obj.get("signal").and_then(|v| v.as_str()) {
                        add_signal_expr_deps(signal_expr, &mut vars)?;
                    }
                }
                _ => {}
            }
        }

        // option expressions in untyped extra properties (e.g. domainRaw)
        for value in self.extra.values() {
            if let Some(signal_expr) = value
                .as_object()
                .and_then(|obj| obj.get("signal"))
                .and_then(|v| v.as_str())
            {
                add_signal_expr_deps(signal_expr, &mut vars)?;
            }
        }

        Ok(vars.into_iter().sorted().collect())
    }

    /// Returns true if this scale uses a domain sort form we intentionally keep client-side.
    ///
    /// Vega accepts object-valued sort definitions that specify a `field` without an aggregate
    /// `op`. For non-`key`/`count` fields this behaves like a no-op in Vega, but can be fragile
    /// in server execution because ordering semantics depend on upstream tuple order. Keep these
    /// scales client-side to avoid extracting potentially mismatched behavior.
    pub fn has_client_only_domain_sort(&self) -> bool {
        match &self.domain {
            Some(ScaleDomainSpec::FieldReference(reference)) => reference
                .sort
                .as_ref()
                .is_some_and(sort_requires_client_execution),
            Some(ScaleDomainSpec::FieldsReference(fields_reference)) => fields_reference
                .sort
                .as_ref()
                .is_some_and(sort_requires_client_execution),
            Some(ScaleDomainSpec::FieldsReferences(fields_references)) => {
                fields_references
                    .sort
                    .as_ref()
                    .is_some_and(sort_requires_client_execution)
                    || fields_references.fields.iter().any(|reference_or_signal| {
                        matches!(
                            reference_or_signal,
                            ScaleDataReferenceOrSignalSpec::Reference(ScaleFieldReferenceSpec {
                                sort: Some(sort),
                                ..
                            }) if sort_requires_client_execution(sort)
                        )
                    })
            }
            _ => false,
        }
    }

    /// Returns true if all signal expressions referenced by this scale are supported by the
    /// server expression runtime.
    pub fn signal_expressions_supported(&self) -> bool {
        self.signal_expressions().iter().all(|expr| {
            parse(expr)
                .map(|parsed| parsed.is_supported())
                .unwrap_or(false)
        })
    }

    /// Returns true if this scale's domain shape is currently supported by server evaluation.
    ///
    /// We fail closed on untyped object-valued domain forms that deserialize into
    /// `ScaleDomainSpec::Value(Object)` (for example heterogeneous `fields` arrays mixing
    /// references, signals, and expression-value arrays). These remain client-side until the
    /// runtime domain resolver supports them.
    pub fn server_domain_shape_supported(&self) -> bool {
        !matches!(
            self.domain.as_ref(),
            Some(ScaleDomainSpec::Value(Value::Object(_)))
        )
    }

    /// Returns true when this scale does not rely on runtime semantics that are intentionally
    /// unsupported for server-side scale evaluation in this phase.
    pub fn server_runtime_semantics_supported(&self) -> bool {
        let domain_mid = self
            .extra
            .get("domainMid")
            .or_else(|| self.extra.get("domain_mid"));

        !domain_mid.is_some_and(|value| !value.is_null())
            && self.server_time_range_semantics_supported()
    }

    fn signal_expressions(&self) -> Vec<&str> {
        let mut signal_exprs = Vec::new();

        // domain
        if let Some(domain) = &self.domain {
            match domain {
                ScaleDomainSpec::FieldsReferences(fields_references) => {
                    for value in &fields_references.fields {
                        if let ScaleDataReferenceOrSignalSpec::Signal(signal_expr) = value {
                            signal_exprs.push(signal_expr.signal.as_str());
                        }
                    }
                }
                ScaleDomainSpec::FieldsSignals(fields_signals) => {
                    for signal_expr in &fields_signals.fields {
                        signal_exprs.push(signal_expr.signal.as_str());
                    }
                }
                ScaleDomainSpec::Signal(signal_expr) => {
                    signal_exprs.push(signal_expr.signal.as_str());
                }
                ScaleDomainSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signal_exprs.push(signal_expr.signal.as_str());
                        }
                    }
                }
                _ => {}
            }
        }

        // range
        if let Some(range) = &self.range {
            match range {
                ScaleRangeSpec::Signal(signal_expr) => {
                    signal_exprs.push(signal_expr.signal.as_str());
                }
                ScaleRangeSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signal_exprs.push(signal_expr.signal.as_str());
                        }
                    }
                }
                ScaleRangeSpec::Value(Value::Object(obj)) => {
                    if let Some(signal_expr) = obj
                        .get("step")
                        .and_then(|step| step.as_object())
                        .and_then(|step| step.get("signal"))
                        .and_then(|v| v.as_str())
                    {
                        signal_exprs.push(signal_expr);
                    }
                }
                _ => {}
            }
        }

        // bins
        if let Some(bins) = &self.bins {
            match bins {
                ScaleBinsSpec::Signal(signal_expr) => {
                    signal_exprs.push(signal_expr.signal.as_str());
                }
                ScaleBinsSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signal_exprs.push(signal_expr.signal.as_str());
                        }
                    }
                }
                ScaleBinsSpec::Value(Value::Object(obj)) => {
                    if let Some(signal_expr) = obj.get("signal").and_then(|v| v.as_str()) {
                        signal_exprs.push(signal_expr);
                    }
                }
                _ => {}
            }
        }

        // option expressions in untyped extra properties (e.g. domainRaw)
        for value in self.extra.values() {
            if let Some(signal_expr) = value
                .as_object()
                .and_then(|obj| obj.get("signal"))
                .and_then(|v| v.as_str())
            {
                signal_exprs.push(signal_expr);
            }
        }

        signal_exprs
    }

    fn server_time_range_semantics_supported(&self) -> bool {
        let scale_type = self.type_.clone().unwrap_or_default();
        if !matches!(scale_type, ScaleTypeSpec::Time | ScaleTypeSpec::Utc) {
            return true;
        }

        let Some(range) = &self.range else {
            return false;
        };

        match range {
            ScaleRangeSpec::Array(values) => values.iter().all(|value| match value {
                ScaleArrayElementSpec::Signal(_) => true,
                ScaleArrayElementSpec::Value(value) => value.is_number(),
            }),
            ScaleRangeSpec::Signal(_) => true,
            ScaleRangeSpec::Value(Value::String(name)) => matches!(name.as_str(), "width" | "height"),
            _ => false,
        }
    }
}

fn add_signal_expr_deps(signal_expr: &str, vars: &mut HashSet<InputVariable>) -> Result<()> {
    let expr = parse(signal_expr)?;
    vars.extend(expr.input_vars());
    Ok(())
}

fn add_domain_value_deps(value: &Value, vars: &mut HashSet<InputVariable>) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                add_domain_value_deps(value, vars)?;
            }
        }
        Value::Object(obj) => add_domain_object_deps(obj, vars)?,
        _ => {}
    }

    Ok(())
}

fn add_domain_object_deps(
    obj: &serde_json::Map<String, Value>,
    vars: &mut HashSet<InputVariable>,
) -> Result<()> {
    if let Some(data) = obj.get("data").and_then(|v| v.as_str()) {
        vars.insert(InputVariable {
            var: Variable::new_data(data),
            propagate: true,
        });
    }

    if let Some(signal_expr) = obj.get("signal").and_then(|v| v.as_str()) {
        add_signal_expr_deps(signal_expr, vars)?;
    }

    for value in obj.values() {
        add_domain_value_deps(value, vars)?;
    }

    Ok(())
}

fn sort_requires_client_execution(sort: &ScaleDataReferenceSort) -> bool {
    match sort {
        ScaleDataReferenceSort::Bool(_) => false,
        ScaleDataReferenceSort::Parameters(params) => {
            params.op.is_none()
                && params
                    .field
                    .as_deref()
                    .is_some_and(|field| field != "key" && field != "count")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScaleTypeSpec {
    // Quantitative Scales
    Linear,
    Log,
    Pow,
    Sqrt,
    Symlog,
    Time,
    Utc,
    Sequential,

    // Discrete Scales
    Ordinal,
    Band,
    Point,

    // Discretizing Scales
    Quantile,
    Quantize,
    Threshold,
    #[serde(rename = "bin-ordinal")]
    BinOrdinal,
}

impl Default for ScaleTypeSpec {
    fn default() -> Self {
        Self::Linear
    }
}

impl ScaleTypeSpec {
    pub fn is_discrete(&self) -> bool {
        use ScaleTypeSpec::*;
        matches!(self, Ordinal | Band | Point)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDomainSpec {
    Array(Vec<ScaleArrayElementSpec>),
    FieldReference(ScaleFieldReferenceSpec),
    FieldsVecStrings(ScaleVecStringsSpec),
    FieldsReference(ScaleFieldsReferenceSpec),
    FieldsReferences(ScaleFieldsReferencesSpec),
    FieldsSignals(ScaleSignalsSpec),
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleFieldsReferencesSpec {
    pub fields: Vec<ScaleDataReferenceOrSignalSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<ScaleDataReferenceSort>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleVecStringsSpec {
    pub fields: Vec<Vec<String>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDataReferenceOrSignalSpec {
    Reference(ScaleFieldReferenceSpec),
    Signal(SignalExpressionSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleFieldReferenceSpec {
    pub data: String,
    pub field: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<ScaleDataReferenceSort>,

    // Need to support sort objects as well as booleans
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub sort: Option<bool>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleFieldsReferenceSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    pub fields: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<ScaleDataReferenceSort>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl ScaleFieldsReferenceSpec {
    pub fn to_field_references(&self) -> Vec<ScaleFieldReferenceSpec> {
        if let Some(data) = &self.data.clone() {
            self.fields
                .iter()
                .map(|f| ScaleFieldReferenceSpec {
                    data: data.clone(),
                    field: f.clone(),
                    sort: self.sort.clone(),
                    extra: Default::default(),
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleSignalsSpec {
    pub fields: Vec<SignalExpressionSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleDataReferenceSort {
    Bool(bool),
    Parameters(ScaleDataReferenceSortParameters),
}

impl Default for ScaleDataReferenceSort {
    fn default() -> Self {
        Self::Bool(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleDataReferenceSortParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op: Option<AggregateOpSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrderSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleArrayElementSpec {
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleBinsSpec {
    Signal(SignalExpressionSpec),
    Array(Vec<ScaleArrayElementSpec>),
    Value(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScaleRangeSpec {
    Array(Vec<ScaleArrayElementSpec>),
    Reference(ScaleFieldReferenceSpec),
    Signal(SignalExpressionSpec),
    Value(Value),
}

#[cfg(test)]
mod tests {
    use super::ScaleSpec;
    use crate::proto::gen::tasks::Variable;
    use serde_json::json;

    #[test]
    fn test_input_vars_include_data_refs_in_mixed_fields_domain_value() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "y",
            "type": "linear",
            "domain": {
                "fields": [
                    {"data": "data_0", "field": "mean_Acceleration"},
                    [10]
                ]
            },
            "range": [0, 100]
        }))
        .unwrap();

        let input_vars = scale.input_vars().unwrap();
        assert!(
            input_vars
                .iter()
                .any(|input_var| input_var.var == Variable::new_data("data_0"))
        );
    }
}
