use crate::proto::gen::tasks::data_url_task::Url;
use crate::proto::gen::tasks::{
    DataSourceTask, DataUrlTask, DataValuesTask, ScaleTask, SignalTask, Variable,
};
use crate::spec::scale::{
    ScaleArrayElementSpec, ScaleBinsSpec, ScaleDataReferenceOrSignalSpec, ScaleDomainSpec,
    ScaleRangeSpec, ScaleSpec,
};
use crate::task_graph::task::{InputVariable, TaskDependencies};
use crate::transform::TransformDependencies;
use itertools::Itertools;
use serde_json::Value;
use std::collections::HashSet;

impl TaskDependencies for DataValuesTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        vars.into_iter().sorted().collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for DataUrlTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Collect input vars from URL signal
        if let Url::Expr(expr) = self.url.as_ref().unwrap() {
            vars.extend(expr.input_vars());
        }

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        vars.into_iter().sorted().collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for DataSourceTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Add input vars from source
        vars.insert(InputVariable {
            var: Variable::new_data(&self.source),
            propagate: true,
        });

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        vars.into_iter().sorted().collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for SignalTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let expr = self.expr.as_ref().unwrap();
        expr.input_vars()
    }
}

impl TaskDependencies for ScaleTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        let Ok(scale): Result<ScaleSpec, _> = serde_json::from_str(&self.spec) else {
            return vars.into_iter().sorted().collect();
        };

        // domain
        if let Some(domain) = &scale.domain {
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
                    for v in &fields_references.fields {
                        match v {
                            ScaleDataReferenceOrSignalSpec::Reference(field_ref) => {
                                vars.insert(InputVariable {
                                    var: Variable::new_data(&field_ref.data),
                                    propagate: true,
                                });
                            }
                            ScaleDataReferenceOrSignalSpec::Signal(signal_expr) => {
                                add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                            }
                        }
                    }
                }
                ScaleDomainSpec::FieldsSignals(fields_signals) => {
                    for signal_expr in &fields_signals.fields {
                        add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                    }
                }
                ScaleDomainSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                }
                ScaleDomainSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                        }
                    }
                }
                _ => {}
            }
        }

        // range
        if let Some(range) = &scale.range {
            match range {
                ScaleRangeSpec::Reference(reference) => {
                    vars.insert(InputVariable {
                        var: Variable::new_data(&reference.data),
                        propagate: true,
                    });
                }
                ScaleRangeSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                }
                ScaleRangeSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
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
                _ => {}
            }
        }

        // bins
        if let Some(bins) = &scale.bins {
            match bins {
                ScaleBinsSpec::Signal(signal_expr) => {
                    add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                }
                ScaleBinsSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            add_signal_expr_deps(signal_expr.signal.as_str(), &mut vars);
                        }
                    }
                }
                _ => {}
            }
        }

        // Also include signal option expressions from untyped extra properties.
        for value in scale.extra.values() {
            if let Some(signal_expr) = value
                .as_object()
                .and_then(|obj| obj.get("signal"))
                .and_then(|v| v.as_str())
            {
                add_signal_expr_deps(signal_expr, &mut vars);
            }
        }

        vars.into_iter().sorted().collect()
    }
}

fn add_signal_expr_deps(signal_expr: &str, vars: &mut HashSet<InputVariable>) {
    if let Ok(expr) = crate::expression::parser::parse(signal_expr) {
        vars.extend(expr.input_vars());
    }
}
