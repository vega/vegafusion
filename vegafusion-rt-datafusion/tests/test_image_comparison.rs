#[macro_use]
extern crate lazy_static;

mod util;

#[cfg(test)]
mod test_image_comparison {
    use std::collections::{HashMap, HashSet};
    use std::convert::TryFrom;
    use serde_json::json;

    use std::fs;
    use std::sync::Arc;
    use datafusion::scalar::ScalarValue;
    use vegafusion_core::data::scalar::ScalarValueHelpers;
    use vegafusion_core::data::table::VegaFusionTable;
    use vegafusion_core::planning::extract::extract_server_data;
    use vegafusion_core::planning::stitch::stitch_specs;
    use vegafusion_core::proto::gen::tasks::{NodeValueIndex, TaskGraph};
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::task_graph::task_graph::ScopedVariable;
    use vegafusion_core::task_graph::task_value::TaskValue;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
    use crate::util::vegajs_runtime::{ExportImageFormat, ExportUpdate, ExportUpdateBatch, ExportUpdateNamespace, vegajs_runtime, Watch, WatchNamespace};

    #[tokio::test(flavor = "multi_thread")]
    async fn test() {
        // let spec_name = "flights_crossfilter_a";
        let spec_name = "bar_colors";

        // Initialize runtime
        let vegajs_runtime = vegajs_runtime();

        // Load spec
        let mut crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).display().to_string();
        let spec_path= format!("{}/tests/specs/sequence/{}/spec.json", crate_dir, spec_name);
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let full_spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Load updates
        let updates_path= format!("{}/tests/specs/sequence/{}/updates.json", crate_dir, spec_name);
        let updates_str = fs::read_to_string(updates_path).unwrap();
        let full_updates: Vec<ExportUpdateBatch> = serde_json::from_str(&updates_str).unwrap();

        // Perform client/server planning
        let mut task_scope = full_spec.to_task_scope().unwrap();
        let mut client_spec = full_spec.clone();
        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope).unwrap();
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec).unwrap();

        // Build task graph
        let tasks = server_spec.to_tasks().unwrap();
        let mut task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        // Collect watch variables and node indices
        let watch_vars = comm_plan.server_to_client.clone();
        let watch_indices: Vec<_> = watch_vars.iter().map(|var| {
            task_graph_mapping.get(var).unwrap()
        }).collect();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(10);

        // Extract the initial values of all of the variables that should be sent from the
        // server to the client
        println!("comm_plan: {:#?}", comm_plan);
        println!("server_spec: {}", serde_json::to_string_pretty(&server_spec).unwrap());
        let mut init = Vec::new();
        for var in &comm_plan.server_to_client {
            let node_index = task_graph_mapping.get(&var).unwrap();
            let value = runtime.get_node_value(
                Arc::new(task_graph.clone()), node_index
            ).await.unwrap();

            init.push(
                ExportUpdate {
                    namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
                    name: var.0.name.clone(),
                    scope: var.1.clone(),
                    value: value.to_json().unwrap(),
                }
            );
        }

        // Build watches for all of the variables that should be sent from the client to the
        // server
        let watches: Vec<_> = comm_plan.client_to_server.iter().map(|t| {
            Watch::try_from(t.clone()).unwrap()
        }).collect();

        // Export sequence with full spec
        let export_sequence_results = vegajs_runtime.export_spec_sequence(
            &full_spec,
            ExportImageFormat::Png,
            Vec::new(),
            full_updates.clone(),
            watches,
        ).unwrap();

        // Save exported PNGs
        for (i, (export_image, _)) in export_sequence_results.iter().enumerate() {
            export_image.save(&format!(
                "{}/tests/output/{}_full{}", crate_dir, spec_name, i
            ), true).unwrap();
        }

        // Update graph with client-to-server watches, and collect values to update the client with
        let mut server_to_client_value_batches: Vec<HashMap<ScopedVariable, TaskValue>> = Vec::new();
        for (_, watch_values) in export_sequence_results.iter().skip(1) {

            // Update graph
            for watch_value in watch_values {
                let variable = watch_value.watch.to_scoped_variable();
                let value = match &watch_value.watch.namespace {
                    WatchNamespace::Signal => {
                        TaskValue::Scalar(ScalarValue::from_json(&watch_value.value).unwrap())
                    }
                    WatchNamespace::Data => {
                        TaskValue::Table(VegaFusionTable::from_json(&watch_value.value, 1024).unwrap())
                    }
                };

                let index = task_graph_mapping.get(&variable).unwrap().node_index as usize;
                task_graph.update_value(index, value).unwrap();
            }

            // Get updated server to client values
            let mut server_to_client_value_batch = HashMap::new();
            for (var, node_index) in watch_vars.iter().zip(&watch_indices) {
                let value = runtime.get_node_value(
                    Arc::new(task_graph.clone()), node_index
                ).await.unwrap();

                server_to_client_value_batch.insert(var.clone(), value);
            }

            server_to_client_value_batches.push(server_to_client_value_batch);
        }

        // Merge the original updates with the original batch updates
        let planned_spec_updates: Vec<_> = full_updates.iter().zip(server_to_client_value_batches).map(
            |(full_export_updates, server_to_client_values)| {
                let server_to_clients_export_updates: Vec<_> = server_to_client_values.iter().map(|(scoped_var, value)| {
                    let json_value = match value {
                        TaskValue::Scalar(value) => value.to_json().unwrap(),
                        TaskValue::Table(value) => value.to_json(),
                    };
                    ExportUpdate {
                        namespace: ExportUpdateNamespace::try_from(scoped_var.0.namespace()).unwrap(),
                        name: scoped_var.0.name.clone(),
                        scope: scoped_var.1.clone(),
                        value: json_value,
                    }
                }).collect();

                let mut total_updates = Vec::new();

                total_updates.extend(server_to_clients_export_updates);
                total_updates.extend(full_export_updates.clone());

                // export_updates
                total_updates
            }
        ).collect();

        println!("client_spec:\n{}", serde_json::to_string_pretty(&client_spec).unwrap());
        println!("init:\n{:?}", init);
        // Export the planned client spec with updates from task graph
        let planned_export_images: Vec<_> = vegajs_runtime.export_spec_sequence(
            &client_spec,
            ExportImageFormat::Png,
            init,
            planned_spec_updates,
            Default::default(),
        ).unwrap().into_iter().map(|(img, _)| img).collect();

        // Export Planned image SVGs
        for (i, server_img) in planned_export_images.into_iter().enumerate() {
            server_img.save(&format!("{}/tests/output/{}_planned{}", crate_dir, spec_name, i), true);
            let (full_img, _) = &export_sequence_results[i];

            let (difference, diff_img) = full_img.compare(&server_img).unwrap();
            if let Some(diff_img) = diff_img {
                let diff_path = format!("{}/tests/output/{}_diff{}.png", crate_dir, spec_name, i);
                fs::write(&diff_path, diff_img).unwrap();
                assert!(false, "Found difference in exported images.\nDiff written to {}", diff_path)
            }
        }
    }
}
