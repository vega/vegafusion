/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::data::dataset::VegaFusionDataset;
use crate::data::scalar::{ScalarValue, ScalarValueHelpers};
use crate::data::table::VegaFusionTable;
use crate::error::{Result, VegaFusionError};
use crate::expression::parser::parse;
use crate::proto::gen::tasks::data_url_task::Url;
use crate::proto::gen::tasks::{
    scan_url_format, DataSourceTask, DataUrlTask, DataValuesTask, ParseFieldSpec, ParseFieldSpecs,
    ScanUrlFormat, Task, TzConfig, Variable,
};
use crate::proto::gen::transforms::TransformPipeline;
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataFormatParseSpec, DataSpec};
use crate::spec::mark::{MarkFacetSpec, MarkSpec};
use crate::spec::scale::{
    ScaleArrayElementSpec, ScaleBinsSpec, ScaleDataReferenceSpec, ScaleDomainSpec, ScaleRangeSpec,
    ScaleSpec,
};
use crate::spec::signal::{SignalOnEventSpec, SignalSpec};
use crate::spec::values::{SignalExpressionSpec, StringOrSignalSpec};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task_value::TaskValue;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::ops::Deref;

#[derive(Clone, Debug, Default)]
pub struct MakeTaskScopeVisitor {
    pub task_scope: TaskScope,
}

impl MakeTaskScopeVisitor {
    pub fn new() -> Self {
        Self {
            task_scope: Default::default(),
        }
    }
}

impl ChartVisitor for MakeTaskScopeVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.data.insert(data.name.clone());
        for sig in data.output_signals() {
            task_scope
                .output_var_defs
                .insert(Variable::new_signal(&sig), Variable::new_data(&data.name));
        }
        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.signals.insert(signal.name.clone());
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.scales.insert(scale.name.clone());
        Ok(())
    }

    fn visit_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Initialize scope for this group level
        let parent_scope = self.task_scope.get_child_mut(&scope[0..scope.len() - 1])?;
        let mut group_scope: TaskScope = Default::default();

        // Check for facet dataset
        if let Some(from) = &mark.from {
            if let Some(facet) = &from.facet {
                group_scope.data.insert(facet.name.clone());
            }
        }

        // Make group itself a dataset
        if let Some(name) = &mark.name {
            parent_scope.data.insert(name.clone());
        }

        parent_scope.children.push(group_scope);

        Ok(())
    }

    fn visit_non_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Named non-group marks can serve as datasets
        if let Some(name) = &mark.name {
            let task_scope = self.task_scope.get_child_mut(scope)?;
            task_scope.data.insert(name.clone());
        }
        Ok(())
    }
}

/// For a spec that is fully supported on the server, collect tasks
#[derive(Clone, Debug)]
pub struct MakeTasksVisitor<'a> {
    pub tasks: Vec<Task>,
    pub tz_config: TzConfig,
    pub datasets: &'a HashMap<String, VegaFusionDataset>,
}

impl<'a> MakeTasksVisitor<'a> {
    pub fn new(tz_config: &TzConfig, datasets: &'a HashMap<String, VegaFusionDataset>) -> Self {
        Self {
            tasks: Default::default(),
            tz_config: tz_config.clone(),
            datasets,
        }
    }
}

impl<'a> ChartVisitor for MakeTasksVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let data_var = Variable::new_data(&data.name);

        // Compute pipeline
        let pipeline = if data.transform.is_empty() {
            None
        } else {
            Some(TransformPipeline::try_from(data.transform.as_slice())?)
        };

        // Extract format
        let format_type = match &data.format {
            Some(format) => {
                let parse = format.parse.as_ref().map(|parse| match parse {
                    DataFormatParseSpec::Object(parse_fields) => {
                        scan_url_format::Parse::Object(ParseFieldSpecs {
                            specs: parse_fields
                                .iter()
                                .map(|(field, datatype)| ParseFieldSpec {
                                    name: field.clone(),
                                    datatype: datatype.clone(),
                                })
                                .collect(),
                        })
                    }
                    DataFormatParseSpec::Auto(parse_mode) => {
                        // Treat any string as auto
                        scan_url_format::Parse::String(parse_mode.clone())
                    }
                });

                Some(ScanUrlFormat {
                    r#type: format.type_.clone(),
                    property: None,
                    header: vec![],
                    delimiter: None,
                    feature: None,
                    parse,
                })
            }
            None => None,
        };

        let task = if let Some(url) = &data.url {
            let mut proto_url = match url {
                StringOrSignalSpec::String(url) => Url::String(url.clone()),
                StringOrSignalSpec::Signal(expr) => {
                    let url_expr = parse(&expr.signal)?;
                    Url::Expr(url_expr)
                }
            };

            // Append fingerprint to URL that references an inline dataset
            if let Url::String(url) = &proto_url {
                if let Some(inline_name) = url.strip_prefix("vegafusion+dataset://") {
                    let inline_name = inline_name.trim().to_string();
                    if let Some(dataset) = self.datasets.get(&inline_name) {
                        proto_url = Url::String(format!("{}#{}", url, dataset.fingerprint()));
                    }
                }
            }

            Task::new_data_url(
                data_var,
                scope,
                DataUrlTask {
                    batch_size: 8096,
                    format_type,
                    pipeline,
                    url: Some(proto_url),
                },
                &self.tz_config,
            )
        } else if let Some(source) = &data.source {
            Task::new_data_source(
                data_var,
                scope,
                DataSourceTask {
                    source: source.clone(),
                    pipeline,
                },
                &self.tz_config,
            )
        } else {
            let values_table = match data.values.as_ref() {
                Some(values) => VegaFusionTable::from_json(values, 1024)?,
                None => {
                    // Treat as empty values array
                    VegaFusionTable::from_json(&Value::Array(Vec::new()), 1)?
                }
            };

            if pipeline.is_none() && format_type.is_none() {
                // If no transforms, treat as regular TaskValue task
                Task::new_value(data_var, scope, TaskValue::Table(values_table))
            } else {
                // Otherwise, create data values task (which supports transforms)
                Task::new_data_values(
                    data_var,
                    scope,
                    DataValuesTask {
                        values: values_table.to_ipc_bytes()?,
                        format_type,
                        pipeline,
                    },
                    &self.tz_config,
                )
            }
        };
        self.tasks.push(task);
        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        let signal_var = Variable::new_signal(&signal.name);

        let task = if let Some(value) = &signal.value {
            let value = TaskValue::Scalar(ScalarValue::from_json(value)?);
            Task::new_value(signal_var, scope, value)
        } else if let Some(update) = &signal.update {
            let expression = parse(update)?;
            Task::new_signal(signal_var, scope, expression, &self.tz_config)
        } else {
            return Err(VegaFusionError::internal(format!(
                "Signal must have an initial value or an update expression: {:#?}",
                signal
            )));
        };

        self.tasks.push(task);
        Ok(())
    }

    fn visit_scale(&mut self, _scale: &ScaleSpec, _scope: &[u32]) -> Result<()> {
        unimplemented!("Scale tasks not yet supported")
    }
}

#[derive(Clone, Debug, Default)]
pub struct DefinitionVarsChartVisitor {
    pub definition_vars: HashSet<ScopedVariable>,
}

impl DefinitionVarsChartVisitor {
    pub fn new() -> Self {
        Self {
            definition_vars: Default::default(),
        }
    }
}

impl ChartVisitor for DefinitionVarsChartVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        self.definition_vars
            .insert((Variable::new_data(&data.name), Vec::from(scope)));
        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        self.definition_vars
            .insert((Variable::new_signal(&signal.name), Vec::from(scope)));
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        self.definition_vars
            .insert((Variable::new_scale(&scale.name), Vec::from(scope)));
        Ok(())
    }
}

/// Collect "update variables" from the spec. These are variables that are updated somewhere other
/// than their definition site
#[derive(Clone, Debug)]
pub struct UpdateVarsChartVisitor<'a> {
    pub task_scope: &'a TaskScope,
    pub update_vars: HashSet<ScopedVariable>,
}

impl<'a> UpdateVarsChartVisitor<'a> {
    pub fn new(task_scope: &'a TaskScope) -> Self {
        Self {
            task_scope,
            update_vars: Default::default(),
        }
    }
}

/// Gather variables that can be updated by the spec (whether or not they are defined in the spec)
impl<'a> ChartVisitor for UpdateVarsChartVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Dataset is an update dependency if it's not an empty stub (without inline values)
        if data.source.is_some()
            || data.on.is_some()
            || data.url.is_some()
            || data.values.is_some()
            || !data.transform.is_empty()
        {
            self.update_vars
                .insert((Variable::new_data(&data.name), Vec::from(scope)));
        }

        // Look for output signals in transforms
        for transform in &data.transform {
            for sig in transform.output_signals() {
                self.update_vars
                    .insert((Variable::new_signal(&sig), Vec::from(scope)));
            }
        }

        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        // Signal is an update variable if it's not an empty stub
        if signal.value.is_some()
            || signal.init.is_some()
            || signal.update.is_some()
            || !signal.on.is_empty()
        {
            self.update_vars
                .insert((Variable::new_signal(&signal.name), Vec::from(scope)));
        }

        // Check for signal expressions that have update dependencies
        // (in particular, the modify expression function)
        let mut expr_strs: Vec<String> = Vec::new();

        if let Some(init) = &signal.init {
            expr_strs.push(init.clone());
        }

        if let Some(update) = &signal.update {
            expr_strs.push(update.clone());
        }

        for on_el in &signal.on {
            expr_strs.push(on_el.update.clone());
            for event_spec in on_el.events.to_vec() {
                if let SignalOnEventSpec::Signal(signal) = event_spec {
                    expr_strs.push(signal.signal.clone());
                }
            }
        }

        // Parse expressions and look for update_vars
        for expr_str in &expr_strs {
            let expr = parse(expr_str)?;
            for var in expr.update_vars() {
                let resolved = self.task_scope.resolve_scope(&var, scope)?;
                self.update_vars.insert((resolved.var, resolved.scope));
            }
        }

        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        // Right now, consider scale variable definition as an update.
        // When scales are supported in TaskGraph, we might need to distinguish between
        // scales with that can be updated, and those that can't
        self.update_vars
            .insert((Variable::new_scale(&scale.name), Vec::from(scope)));
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct InputVarsChartVisitor<'a> {
    pub task_scope: &'a TaskScope,
    pub input_vars: HashSet<ScopedVariable>,
}

impl<'a> InputVarsChartVisitor<'a> {
    pub fn new(task_scope: &'a TaskScope) -> Self {
        Self {
            task_scope,
            input_vars: Default::default(),
        }
    }

    fn process_mark_from(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Handle from data
        if let Some(from) = &mark.from {
            if let Some(data) = &from.data {
                let data_var = Variable::new_data(data);
                let resolved = self.task_scope.resolve_scope(&data_var, scope)?;
                self.input_vars.insert((data_var, resolved.scope));
            }

            if let Some(MarkFacetSpec { data, .. }) = &from.facet {
                let data_var = Variable::new_data(data);
                let resolved = self.task_scope.resolve_scope(&data_var, scope)?;
                self.input_vars.insert((data_var, resolved.scope));
            }
        }

        Ok(())
    }
}

impl<'a> ChartVisitor for InputVarsChartVisitor<'a> {
    fn visit_chart(&mut self, chart: &ChartSpec) -> Result<()> {
        // Handle signals in title
        if let Some(title) = &chart.title {
            if let Some(text) = &title.text {
                for input_var in text.input_vars()? {
                    let resolved = self.task_scope.resolve_scope(&input_var.var, &[])?;
                    let var = resolved.output_var.unwrap_or(resolved.var);
                    self.input_vars.insert((var, resolved.scope));
                }
            }
        }
        Ok(())
    }

    fn visit_non_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Handle from data/facet of group mark
        self.process_mark_from(mark, scope)?;

        // Handle signals in encodings
        if let Some(v) = &mark.encode {
            for encodings in v.encodings.values() {
                for encoding in encodings.channels.values() {
                    for channel in encoding.to_vec() {
                        for signal in vec![&channel.signal, &channel.test].into_iter().flatten() {
                            let expr = parse(signal)?;
                            for input_var in expr.input_vars() {
                                let var = input_var.var;
                                let resolved = self.task_scope.resolve_scope(&var, scope)?;
                                self.input_vars.insert((var, resolved.scope));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn visit_group_mark(&mut self, mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Handle from data/facet of group mark
        self.process_mark_from(mark, scope)?;

        // Handle signals in title
        if let Some(title) = &mark.title {
            if let Some(text) = &title.text {
                for input_var in text.input_vars()? {
                    let resolved = self.task_scope.resolve_scope(&input_var.var, &[])?;
                    let var = resolved.output_var.unwrap_or(resolved.var);
                    self.input_vars.insert((var, resolved.scope));
                }
            }
        }

        Ok(())
    }

    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        // Look for input vars in transforms
        for transform in &data.transform {
            for input_var in transform.deref().input_vars()?.into_iter().map(|iv| iv.var) {
                let resolved = self.task_scope.resolve_scope(&input_var, scope)?;
                self.input_vars.insert((input_var, resolved.scope));
            }
        }

        // Add input variable for source
        if let Some(source) = &data.source {
            let source_var = Variable::new_data(source);
            let resolved = self.task_scope.resolve_scope(&source_var, scope)?;
            self.input_vars.insert((source_var, resolved.scope));
        }

        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        // Collect all expression strings used in the signal definition
        let mut expr_strs: Vec<String> = Vec::new();

        if let Some(init) = &signal.init {
            expr_strs.push(init.clone());
        }

        if let Some(update) = &signal.update {
            expr_strs.push(update.clone());
        }

        for on_el in &signal.on {
            expr_strs.push(on_el.update.clone());
            for event_spec in on_el.events.to_vec() {
                if let SignalOnEventSpec::Signal(signal) = event_spec {
                    expr_strs.push(signal.signal.clone());
                }
            }
        }

        // Parse expressions and look for input_vars
        for expr_str in &expr_strs {
            let expr = parse(expr_str)?;
            for var in expr.input_vars() {
                let resolved = self.task_scope.resolve_scope(&var.var, scope)?;
                self.input_vars.insert((var.var, resolved.scope));
            }
        }

        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let mut references: Vec<ScaleDataReferenceSpec> = Vec::new();
        let mut signals: Vec<SignalExpressionSpec> = Vec::new();

        // domain
        if let Some(domain) = &scale.domain {
            match domain {
                ScaleDomainSpec::FieldReference(reference) => {
                    references.push(reference.clone());
                }
                ScaleDomainSpec::FieldsReference(field_references) => {
                    references.extend(field_references.fields.clone());
                }
                ScaleDomainSpec::FieldsSignals(fields_signals) => {
                    signals.extend(fields_signals.fields.clone());
                }
                ScaleDomainSpec::Signal(signal_expr) => {
                    signals.push(signal_expr.clone());
                }
                ScaleDomainSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signals.push(signal_expr.clone());
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
                    references.push(reference.clone());
                }
                ScaleRangeSpec::Signal(signal_expr) => {
                    signals.push(signal_expr.clone());
                }
                ScaleRangeSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signals.push(signal_expr.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        // bins
        if let Some(bins) = &scale.bins {
            match bins {
                ScaleBinsSpec::Signal(signal_expr) => {
                    signals.push(signal_expr.clone());
                }
                ScaleBinsSpec::Array(arr) => {
                    for el in arr {
                        if let ScaleArrayElementSpec::Signal(signal_expr) = el {
                            signals.push(signal_expr.clone());
                        }
                    }
                }
                _ => {}
            }
        }

        // Process references
        for reference in &references {
            // Resolve referenced data
            let reference_var = Variable::new_data(&reference.data);
            let resolved = self.task_scope.resolve_scope(&reference_var, scope)?;
            self.input_vars.insert((reference_var, resolved.scope));
        }

        // Process signals
        for sig in &signals {
            let expr = parse(&sig.signal)?;
            for input_var in expr.input_vars() {
                let resolved = self.task_scope.resolve_scope(&input_var.var, scope)?;
                self.input_vars.insert((input_var.var, resolved.scope));
            }
        }

        Ok(())
    }
}
