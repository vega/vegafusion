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
    use vegafusion_core::proto::gen::tasks::TaskGraph;
    use vegafusion_core::spec::chart::ChartSpec;
    use vegafusion_core::task_graph::task_graph::ScopedVariable;
    use vegafusion_core::task_graph::task_value::TaskValue;
    use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
    use crate::util::vegajs_runtime::{ExportImageFormat, ExportUpdate, ExportUpdateBatch, ExportUpdateNamespace, vegajs_runtime, Watch, WatchNamespace};

    #[tokio::test(flavor = "multi_thread")]
    async fn test() {
        let spec_name = "flights_crossfilter_a";

        // Load spec
        let mut crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).display().to_string();
        let spec_path= format!("{}/tests/specs/{}.json", crate_dir, spec_name);
        let spec_str = fs::read_to_string(spec_path).unwrap();
        let full_spec: ChartSpec = serde_json::from_str(&spec_str).unwrap();

        // Perform client/server planning
        let mut task_scope = full_spec.to_task_scope().unwrap();
        let mut client_spec = full_spec.clone();
        let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope).unwrap();
        let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec).unwrap();

        // Build task graph
        let tasks = server_spec.to_tasks().unwrap();
        let mut task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
        let task_graph_mapping = task_graph.build_mapping();

        // Initialize task graph runtime
        let runtime = TaskGraphRuntime::new(10);

        // Perform full_spec export with watches
        let full_updates: Vec<ExportUpdateBatch> = vec![
            vec![],
            vec![
                ExportUpdate {
                    namespace: ExportUpdateNamespace::Signal,
                    name: "brush_x".to_string(),
                    scope: vec![0],
                    value: json!([0, 50])
                }
            ],
            vec![
                ExportUpdate {
                    namespace: ExportUpdateNamespace::Signal,
                    name: "brush_x".to_string(),
                    scope: vec![0],
                    value: json!([50, 150])
                }
            ],
            // vec![
            //     ExportUpdate {
            //         namespace: ExportUpdateNamespace::Signal,
            //         name: "brush_x".to_string(),
            //         scope: vec![0],
            //         value: json!([70, 120])
            //     },
            //     ExportUpdate {
            //         namespace: ExportUpdateNamespace::Signal,
            //         name: "brush_x".to_string(),
            //         scope: vec![1],
            //         value: json!([40, 80])
            //     },
            // ],
            // vec![
            //     ExportUpdate {
            //         namespace: ExportUpdateNamespace::Signal,
            //         name: "brush_x".to_string(),
            //         scope: vec![0],
            //         value: json!([0, 0])
            //     }
            // ],
            // vec![
            //     ExportUpdate {
            //         namespace: ExportUpdateNamespace::Signal,
            //         name: "brush_x".to_string(),
            //         scope: vec![1],
            //         value: json!([0, 0])}
            // ],
        ];

        // Build client to server watches
        let watches: Vec<_> = comm_plan.client_to_server.iter().map(|t| {
            Watch::try_from(t.clone()).unwrap()
        }).collect();

        // Export sequence for full spec
        let vegajs_runtime = vegajs_runtime();
        let full_export_results = vegajs_runtime.export_spec_sequence(
            &full_spec,
            ExportImageFormat::Png,
            full_updates.clone(),
            watches,
        ).unwrap();

        // Export full image SVGs
        for (i, (export_image, _)) in full_export_results.iter().enumerate() {
            export_image.save(&format!("{}/tests/output/crossfilter_full{}", crate_dir, i), true).unwrap();
        }

        let mut all_updates: Vec<HashMap<ScopedVariable, TaskValue>> = Vec::new();

        // Update task graph with watched value updates
        let send_watch: HashSet<_> = comm_plan.server_to_client.clone().into_iter().collect();
        let watch_indices: Vec<_> = send_watch.iter().map(|var| {
            task_graph_mapping.get(var).unwrap()
        }).collect();

        for (_, update_batch) in full_export_results.iter().skip(0) {

            // Update graph
            for watch_value in update_batch {
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

            // Get updated node values
            let mut send_updated = HashMap::new();
            for (var, node_index) in send_watch.iter().zip(&watch_indices) {
                let value = runtime.get_node_value(
                    Arc::new(task_graph.clone()), node_index
                ).await.unwrap();

                send_updated.insert(var.clone(), value);
            }

            all_updates.push(send_updated);
        }

        // Merge send updates with the original batch updates
        let planned_updates: Vec<_> = full_updates.iter().zip(all_updates).map(
            |(export_updates, send_updates)| {
                let send_export_updates: Vec<_> = send_updates.iter().map(|(scoped_var, value)| {
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

                // let mut export_updates = export_updates;
                let mut total_updates = Vec::new();

                total_updates.extend(send_export_updates);
                total_updates.extend(export_updates.clone());

                // export_updates
                total_updates
            }
        ).collect();

        // Export client spec with updates from task graph
        println!("{}", serde_json::to_string_pretty(&client_spec).unwrap());

        println!("{:#?}", planned_updates);
        let planned_export_images: Vec<_> = vegajs_runtime.export_spec_sequence(
            &client_spec,
            ExportImageFormat::Png,
            planned_updates,
            Default::default(),
        ).unwrap().into_iter().map(|(img, _)| img).collect();

        // Export Planned image SVGs
        for (i, server_img) in planned_export_images.into_iter().enumerate() {
            server_img.save(&format!("{}/tests/output/crossfilter_planned{}", crate_dir, i), true);
            // let (full_img, _) = &full_export_results[i];

            // let (difference, diff_img) = full_img.compare(&server_img).unwrap();
            // println!("{} difference: {}", i, difference);
            // if let Some(diff_img) = diff_img {
            //     fs::write(&format!("{}/tests/output/crossfilter_diff{}.png", crate_dir, i), diff_img).unwrap();
            // }
        }
    }
}
