use crate::error::Result;
use crate::planning::dependency_graph::{get_supported_data_variables, scoped_var_for_input_var};
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};
use crate::spec::mark::MarkSpec;

use crate::spec::scale::ScaleSpec;
use crate::spec::signal::SignalSpec;
use crate::task_graph::scope::TaskScope;

use crate::task_graph::graph::ScopedVariable;

use crate::planning::plan::PlannerConfig;
use std::collections::{HashMap, HashSet};

pub fn extract_server_data(
    client_spec: &mut ChartSpec,
    task_scope: &mut TaskScope,
    config: &PlannerConfig,
) -> Result<ChartSpec> {
    let supported_vars = get_supported_data_variables(client_spec, config)?;

    let mut extract_server_visitor =
        ExtractServerDependenciesVisitor::new(supported_vars, task_scope, config);
    client_spec.walk_mut(&mut extract_server_visitor)?;

    Ok(extract_server_visitor.server_spec)
}

#[derive(Debug)]
pub struct ExtractServerDependenciesVisitor<'a> {
    pub server_spec: ChartSpec,
    supported_vars: HashMap<ScopedVariable, DependencyNodeSupported>,
    task_scope: &'a mut TaskScope,
    planner_config: &'a PlannerConfig,
}

impl<'a> ExtractServerDependenciesVisitor<'a> {
    pub fn new(
        supported_vars: HashMap<ScopedVariable, DependencyNodeSupported>,
        task_scope: &'a mut TaskScope,
        planner_config: &'a PlannerConfig,
    ) -> Self {
        let server_spec: ChartSpec = ChartSpec {
            schema: "https://vega.github.io/schema/vega/v5.json".into(),
            ..Default::default()
        };
        Self {
            server_spec,
            supported_vars,
            task_scope,
            planner_config,
        }
    }
}

impl MutChartVisitor for ExtractServerDependenciesVisitor<'_> {
    /// Extract data definitions, splitting partially supported transform pipelines
    fn visit_data(&mut self, data: &mut DataSpec, scope: &[u32]) -> Result<()> {
        let data_var: ScopedVariable = (Variable::new_data(&data.name), Vec::from(scope));
        match self.supported_vars.get(&data_var) {
            Some(DependencyNodeSupported::PartiallySupported) => {
                // Split transforms at first unsupported transform.
                // Note: There could be supported transforms in the client_tx after an unsupported
                // transform.

                // Count the number of leading supported transforms. These are transforms that
                // are themselves supported with all supported input dependencies
                let mut pipeline_vars = HashSet::new();
                let mut num_supported = 0;
                'outer: for (i, tx) in data.transform.iter().enumerate() {
                    if tx.supported_and_allowed(self.planner_config, self.task_scope, scope) {
                        if let Ok(input_vars) = tx.input_vars() {
                            for input_var in input_vars {
                                if let Ok(scoped_source_var) =
                                    scoped_var_for_input_var(&input_var, scope, self.task_scope)
                                {
                                    if !pipeline_vars.contains(&scoped_source_var)
                                        && !self.supported_vars.contains_key(&scoped_source_var)
                                    {
                                        // Dependency is not supported and it was not produced earlier in the transform pipeline
                                        break 'outer;
                                    }
                                } else {
                                    // Failed to get input vars for transform (e.g. expression parse failure)
                                    break 'outer;
                                }
                            }
                        }
                        // Add output signals so we know they are available later
                        for sig in &tx.output_signals() {
                            pipeline_vars.insert((Variable::new_signal(sig), Vec::from(scope)));
                        }
                    } else {
                        // Full transform not supported
                        break 'outer;
                    }
                    num_supported = i + 1;
                }

                let server_tx: Vec<_> = Vec::from(&data.transform[..num_supported]);
                let client_tx: Vec<_> = Vec::from(&data.transform[num_supported..]);

                // Compute new name for server data
                let mut server_name = data.name.clone();
                server_name.insert_str(0, "_server_");

                // Clone data for use on server (with updated name)
                let mut server_data = data.clone();
                server_data.name = server_name.clone();
                server_data.transform = server_tx;

                let server_signals = server_data.output_signals();
                // Update server spec
                if scope.is_empty() {
                    self.server_spec.data.push(server_data)
                } else {
                    let server_group = self.server_spec.get_nested_group_mut(scope)?;
                    server_group.data.push(server_data);
                }

                // Update client data spec:
                //   - Same name
                //   - Add source of server
                //   - Update remaining transforms
                data.source = Some(server_name.clone());
                data.format = None;
                data.values = None;
                data.transform = client_tx;
                data.on = None;
                data.url = None;

                // Update scope
                //  - Add new data variable to task scope
                self.task_scope
                    .add_variable(&Variable::new_data(&server_name), scope)?;

                // - Handle signals generated by transforms that have been moved to the server spec
                for sig in &server_signals {
                    self.task_scope.remove_data_signal(sig, scope)?;
                    self.task_scope.add_data_signal(&server_name, sig, scope)?;
                }
            }
            Some(DependencyNodeSupported::Supported) => {
                // Add clone of full server data
                let server_data = data.clone();
                if scope.is_empty() {
                    self.server_spec.data.push(server_data)
                } else {
                    let server_group = self.server_spec.get_nested_group_mut(scope)?;
                    server_group.data.push(server_data);
                }

                if data.is_selection_store() {
                    // Don' clear inline values from client for _store datasets
                } else {
                    // Clear everything except name from client spec
                    data.format = None;
                    data.source = None;
                    data.values = None;
                    data.transform = Vec::new();
                    data.on = None;
                    data.url = None;
                }
            }
            _ => {
                // Nothing to do
            }
        }

        Ok(())
    }

    fn visit_signal(&mut self, signal: &mut SignalSpec, scope: &[u32]) -> Result<()> {
        // check if signal is supported
        let scoped_signal_var = (Variable::new_signal(&signal.name), Vec::from(scope));
        if matches!(
            self.supported_vars.get(&scoped_signal_var),
            Some(DependencyNodeSupported::Supported)
        ) {
            // Move signal to server
            let mut server_signal = signal.clone();
            server_signal.on = vec![];
            server_signal.bind = None;
            if scope.is_empty() {
                self.server_spec.signals.push(server_signal)
            } else {
                let server_group = self.server_spec.get_nested_group_mut(scope)?;
                server_group.signals.push(server_signal);
            }

            // What should be cleared from client signal?
            // signal.init = None;
            // signal.update = None;
            // signal.on = Default::default();
        }
        Ok(())
    }

    fn visit_scale(&mut self, scale: &mut ScaleSpec, scope: &[u32]) -> Result<()> {
        let scoped_scale_var = (Variable::new_scale(&scale.name), Vec::from(scope));
        if matches!(
            self.supported_vars.get(&scoped_scale_var),
            Some(DependencyNodeSupported::Supported)
        ) {
            let server_scale = scale.clone();
            if scope.is_empty() {
                self.server_spec.scales.push(server_scale);
            } else {
                let server_group = self.server_spec.get_nested_group_mut(scope)?;
                server_group.scales.push(server_scale);
            }
        }
        Ok(())
    }

    fn visit_group_mark(&mut self, _mark: &mut MarkSpec, scope: &[u32]) -> Result<()> {
        // Initialize group mark in server spec
        let parent_scope = &scope[..scope.len() - 1];
        let new_group = MarkSpec {
            type_: "group".to_string(),
            name: None,
            from: None,
            sort: None,
            encode: None,
            data: vec![],
            signals: vec![],
            marks: vec![],
            scales: vec![],
            axes: vec![],
            transform: vec![],
            title: None,
            extra: Default::default(),
        };
        if parent_scope.is_empty() {
            self.server_spec.marks.push(new_group);
        } else {
            let parent_group = self.server_spec.get_nested_group_mut(parent_scope)?;
            parent_group.marks.push(new_group);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::planning::extract::extract_server_data;
    use crate::planning::plan::PlannerConfig;
    use crate::spec::chart::ChartSpec;
    use serde_json::json;

    fn chart_with_scale_formula_data() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"value": 1}],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.value)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 1], "range": [0, 100]}
            ]
        }))
        .unwrap()
    }

    fn chart_with_client_only_scale_sort_formula_data() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "domain_source",
                    "values": [{"k": "b", "other": 2}, {"k": "a", "other": 1}]
                },
                {
                    "name": "source",
                    "values": [{"value": 1}, {"value": 2}],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.value)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "band",
                    "domain": {"data": "domain_source", "field": "k", "sort": {"field": "other", "order": "descending"}},
                    "range": [0, 100]
                }
            ]
        }))
        .unwrap()
    }

    fn chart_with_unsupported_scale_signal_expression() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "width": 200,
            "data": [
                {
                    "name": "source",
                    "values": [{"value": 1}],
                    "transform": [
                        {"type": "formula", "expr": "scale('x', datum.value)", "as": "scaled"}
                    ]
                }
            ],
            "scales": [
                {
                    "name": "x",
                    "type": "linear",
                    "domain": [0, 1],
                    "range": [0, {"signal": "foo(width)"}]
                }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn test_scale_dependent_pipeline_stays_client_when_scale_copy_disabled() {
        let mut client_spec = chart_with_scale_formula_data();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = false;

        let server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();

        assert!(server_spec.data.is_empty());
        assert!(server_spec.scales.is_empty());
        assert_eq!(client_spec.data[0].transform.len(), 1);
    }

    #[test]
    fn test_scale_copied_and_pipeline_extractable_when_scale_copy_enabled() {
        let mut client_spec = chart_with_scale_formula_data();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;

        let server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();

        assert_eq!(server_spec.scales.len(), 1);
        assert_eq!(server_spec.scales[0].name, "x");
        assert_eq!(server_spec.data.len(), 1);
        assert_eq!(server_spec.data[0].name, "source");
        assert_eq!(server_spec.data[0].transform.len(), 1);

        assert_eq!(client_spec.data[0].name, "source");
        assert!(client_spec.data[0].transform.is_empty());
        assert!(client_spec.data[0].values.is_none());
    }

    #[test]
    fn test_scale_with_non_aggregated_sort_field_without_op_stays_client() {
        let mut client_spec = chart_with_client_only_scale_sort_formula_data();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;

        let server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();

        assert!(server_spec.scales.is_empty());
        assert!(server_spec.data.iter().any(|d| d.name == "domain_source"));
        let server_source = server_spec
            .data
            .iter()
            .find(|d| d.name == "_server_source")
            .expect("expected partially extracted server source");
        assert!(server_source.transform.is_empty());
        let source = client_spec
            .data
            .iter()
            .find(|d| d.name == "source")
            .expect("expected source dataset");
        assert_eq!(source.transform.len(), 1);
    }

    #[test]
    fn test_scale_with_unsupported_signal_expression_stays_client() {
        let mut client_spec = chart_with_unsupported_scale_signal_expression();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;

        let server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();
        assert!(server_spec.scales.is_empty());

        let source = client_spec
            .data
            .iter()
            .find(|d| d.name == "source")
            .expect("expected source dataset");
        assert_eq!(source.transform.len(), 1);
    }

    #[test]
    fn test_mirrored_signals_not_copied_to_server_spec() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "signals": [
                {"name": "compute_sig", "value": 2},
                {
                    "name": "interactive_sig",
                    "value": 0,
                    "on": [{"events": "pointermove", "update": "event.x"}]
                },
                {
                    "name": "bound_sig",
                    "value": 1,
                    "bind": {"input": "range", "min": 0, "max": 10}
                }
            ],
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 1}],
                    "transform": [
                        {"type": "formula", "expr": "datum.v + compute_sig", "as": "out"}
                    ]
                }
            ]
        }))
        .unwrap();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;

        let server_spec = extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();
        let server_signal_names: Vec<_> = server_spec
            .signals
            .iter()
            .map(|s| s.name.as_str())
            .collect();

        assert!(server_signal_names.contains(&"compute_sig"));
        assert!(!server_signal_names.contains(&"interactive_sig"));
        assert!(!server_signal_names.contains(&"bound_sig"));
    }
}
