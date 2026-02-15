use crate::error::{Result, VegaFusionError};
use crate::proto::gen::tasks::VariableNamespace;
use crate::spec::chart::ChartSpec;
use crate::spec::data::DataSpec;
use crate::spec::signal::SignalSpec;
use crate::spec::values::MissingNullOrValue;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use itertools::Itertools;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct CommPlan {
    pub server_to_client: Vec<ScopedVariable>,
    pub client_to_server: Vec<ScopedVariable>,
}

pub fn stitch_specs(
    task_scope: &TaskScope,
    server_spec: &mut ChartSpec,
    client_spec: &mut ChartSpec,
    keep_variables: &[ScopedVariable],
) -> Result<CommPlan> {
    // Get client spec variable types
    let client_defs: HashSet<_> = client_spec.definition_vars()?.into_iter().collect();
    let client_inputs: HashSet<_> = client_spec.input_vars(task_scope)?.into_iter().collect();
    let client_updates: HashSet<_> = client_spec.update_vars(task_scope)?.into_iter().collect();

    // Get server spec variable types
    let server_defs: HashSet<_> = server_spec.definition_vars()?.into_iter().collect();
    let server_inputs: HashSet<_> = server_spec.input_vars(task_scope)?.into_iter().collect();
    let server_updates: HashSet<_> = server_spec.update_vars(task_scope)?.into_iter().collect();

    // Determine communication requirements
    let mut server_to_client: HashSet<_> = client_inputs
        .intersection(&server_updates)
        .cloned()
        .collect();

    // Add keep variables to server_to_client to make sure they are included in the client spec
    for keep_var in keep_variables.iter() {
        if server_defs.contains(keep_var) || server_updates.contains(keep_var) {
            server_to_client.insert(keep_var.clone());
        } else if !(client_defs.contains(keep_var) || client_updates.contains(keep_var)) {
            return Err(VegaFusionError::pre_transform(format!(
                "Keep variable does not exist: {keep_var:?}"
            )));
        }
    }

    // Scale variables are never communicated in this phase
    server_to_client.retain(|var| !matches!(var.0.namespace(), VariableNamespace::Scale));

    let client_to_server: HashSet<_> = server_inputs
        .intersection(&client_updates)
        .cloned()
        .collect();

    // If a variable is updated on both client and server, don't send it.
    // This can happen when a signal with an update expression resides on both client and server
    let client_to_server: HashSet<_> = client_to_server
        .difference(&server_updates)
        .cloned()
        .collect();
    let client_to_server: HashSet<_> = client_to_server
        .into_iter()
        .filter(|var| !matches!(var.0.namespace(), VariableNamespace::Scale))
        .collect();

    // determine stub definitions that needs to be added to server and client specs
    let server_stubs: HashSet<_> = client_to_server.difference(&server_defs).cloned().collect();
    let client_stubs: HashSet<_> = server_to_client.difference(&client_defs).cloned().collect();

    // Add server stubs
    for stub_id in server_stubs {
        make_stub(stub_id, server_spec, client_spec)?;
    }

    // Add client stubs
    for stub_id in client_stubs {
        make_stub(stub_id, client_spec, server_spec)?;
    }

    // Return plan which specifies which signals/data need to be communicated between client and server
    Ok(CommPlan {
        server_to_client: server_to_client.into_iter().sorted().collect(),
        client_to_server: client_to_server.into_iter().sorted().collect(),
    })
}

fn make_stub(
    stub_var: ScopedVariable,
    to_spec: &mut ChartSpec,
    from_spec: &ChartSpec,
) -> Result<()> {
    let stub_name = stub_var.0.name.clone();
    let stub_path = stub_var.1;
    match stub_var.0.namespace() {
        VariableNamespace::Signal => {
            // Get initial value from client spec, if any
            let stub_value = from_spec
                .get_nested_signal(&stub_path, &stub_name)
                .ok()
                .and_then(|s| s.value.as_option());

            let new_signal_spec = SignalSpec {
                name: stub_name,
                init: None,
                update: None,
                value: MissingNullOrValue::from(stub_value),
                on: vec![],
                bind: None,
                extra: Default::default(),
            };

            to_spec.add_nested_signal(&stub_path, new_signal_spec, Some(0))?;
        }
        VariableNamespace::Data => {
            // Get initial value from client spec, if any. Initial value is only valid if
            // there are no transforms
            let stub_spec = from_spec
                .get_nested_data(stub_path.as_slice(), &stub_name)
                .ok();
            let stub_values: Option<Value> = stub_spec.and_then(|s| {
                if s.transform.is_empty() {
                    None
                } else {
                    s.values.clone()
                }
            });

            let new_data_spec = DataSpec {
                name: stub_name.clone(),
                source: None,
                url: None,
                format: None,
                values: stub_values,
                transform: vec![],
                on: None,
                extra: Default::default(),
            };

            to_spec.add_nested_data(&stub_path, new_data_spec, Some(0))?;
        }
        VariableNamespace::Scale => {
            return Err(VegaFusionError::internal("Scale stubs not yet supported"))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::planning::stitch::stitch_specs;
    use crate::proto::gen::tasks::Variable;
    use crate::spec::chart::ChartSpec;
    use serde_json::json;

    #[test]
    fn test_scale_namespace_excluded_from_comm_plan() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "signals": [
                {"name": "client_signal", "update": "scale('x', 1)"}
            ],
            "scales": [
                {"name": "x"}
            ]
        }))
        .unwrap();

        let mut server_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "signals": [
                {"name": "server_signal", "update": "scale('x', 2)"}
            ],
            "scales": [
                {"name": "x"}
            ]
        }))
        .unwrap();

        let task_scope = client_spec.to_task_scope().unwrap();
        let keep_variables = vec![(Variable::new_scale("x"), Vec::new())];
        let comm_plan = stitch_specs(
            &task_scope,
            &mut server_spec,
            &mut client_spec,
            &keep_variables,
        )
        .unwrap();

        assert!(comm_plan.server_to_client.iter().all(|var| !matches!(
            var.0.namespace(),
            crate::proto::gen::tasks::VariableNamespace::Scale
        )));
        assert!(comm_plan.client_to_server.iter().all(|var| !matches!(
            var.0.namespace(),
            crate::proto::gen::tasks::VariableNamespace::Scale
        )));
    }

    #[test]
    fn test_client_to_server_scale_dependency_filtered() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "scales": [
                {"name": "x"}
            ]
        }))
        .unwrap();

        let mut server_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "signals": [
                {"name": "server_signal", "update": "scale('x', 2)"}
            ]
        }))
        .unwrap();

        let task_scope = client_spec.to_task_scope().unwrap();
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec, &[]).unwrap();

        assert!(comm_plan.server_to_client.is_empty());
        assert!(comm_plan.client_to_server.is_empty());
    }
}
