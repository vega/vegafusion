use crate::expression::parser::parse;
use crate::planning::plan::PlannerConfig;
use crate::spec::data::DependencyNodeSupported;
use crate::spec::values::{MissingNullOrValue, StringOrStringList};
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub init: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<String>,

    #[serde(default, skip_serializing_if = "MissingNullOrValue::is_missing")]
    pub value: MissingNullOrValue,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<SignalOnSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind: Option<Value>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl SignalSpec {
    pub fn supported(
        &self,
        planner_config: &PlannerConfig,
        task_scope: &TaskScope,
        scope: &[u32],
    ) -> DependencyNodeSupported {
        if !self.value.is_missing() {
            return DependencyNodeSupported::Supported;
        } else if let Some(expr) = &self.update {
            if self.on.is_empty() {
                if let Ok(expression) = parse(expr) {
                    // Check if signal has direct dependency on client-only variable
                    for input_var in expression.input_vars() {
                        if let Ok(resolved) = task_scope.resolve_scope(&input_var.var, scope) {
                            let resolved_var = (resolved.var, resolved.scope);
                            if planner_config.client_only_vars.contains(&resolved_var) {
                                return DependencyNodeSupported::Unsupported;
                            }
                        }
                    }
                    if expression.is_supported() {
                        return DependencyNodeSupported::Supported;
                    }
                }
            }
        }
        // TODO: add init once we decide how to differentiate it from update in task graph
        DependencyNodeSupported::Unsupported
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnSpec {
    pub events: SignalOnEventSpecOrList,
    pub update: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SignalOnEventSpecOrList {
    List(Vec<SignalOnEventSpec>),
    Scalar(SignalOnEventSpec),
}

impl SignalOnEventSpecOrList {
    pub fn to_vec(&self) -> Vec<SignalOnEventSpec> {
        match self {
            SignalOnEventSpecOrList::List(event_specs) => event_specs.clone(),
            SignalOnEventSpecOrList::Scalar(event_spec) => vec![event_spec.clone()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SignalOnEventSpec {
    Signal(SignalOnSignalEvent),
    Scale(SignalOnScaleEvent),
    Source(SignalOnSourceEvent),
    Selector(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalOnSignalEvent {
    pub signal: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalOnScaleEvent {
    pub scale: String,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalOnSourceEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub markname: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<StringOrStringList>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub between: Option<Vec<SignalOnEventSpec>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use crate::spec::signal::SignalSpec;

    #[test]
    fn test_signal_null_value_not_dropped() {
        // No value is valid
        let s = r#"{"name":"foo"}"#;
        let sig: SignalSpec = serde_json::from_str(s).unwrap();
        let res = serde_json::to_string(&sig).unwrap();
        assert_eq!(res, s);

        // Null value should not be dropped
        let s = r#"{"name":"foo","value":null}"#;
        let sig: SignalSpec = serde_json::from_str(s).unwrap();
        let res = serde_json::to_string(&sig).unwrap();
        assert_eq!(res, s);
    }
}
