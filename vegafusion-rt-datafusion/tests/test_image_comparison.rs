#[macro_use]
extern crate lazy_static;

mod util;

use crate::util::vegajs_runtime::{
    vegajs_runtime, ExportImageFormat, ExportUpdate, ExportUpdateBatch, ExportUpdateNamespace,
    Watch, WatchNamespace, WatchPlan,
};
use datafusion::scalar::ScalarValue;
use rstest::rstest;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::sync::Arc;
use tokio::runtime::Runtime;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::planning::extract::extract_server_data;
use vegafusion_core::planning::stitch::stitch_specs;
use vegafusion_core::proto::gen::tasks::TaskGraph;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::task_graph::ScopedVariable;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[cfg(test)]
mod test_image_comparison_mocks {
    use super::*;

    #[rstest(
        spec_name,
        case("stacked_bar"),
        case("bar_colors"),
        case("imdb_histogram"),
        case("flights_crossfilter_a"),
        case("log_scaled_histogram"),
        case("non_linear_histogram"),
        case("relative_frequency_histogram"),
        case("kde_movies"),
        case("2d_circles_histogram_imdb"),
        case("2d_histogram_imdb"),
        case("cumulative_window_imdb"),
        case("density_and_cumulative_histograms"),
        case("mean_strip_plot_movies"),
        case("table_heatmap_cars"),
        case("difference_from_mean"),
        case("nested_concat_align"),
        case("imdb_dashboard_cross_height"),
        case("stacked_bar_weather_year"),
        case("stacked_bar_weather_month"),
        case("stacked_bar_normalize"),
        case("layer_bar_labels_grey"),
        case("bar_month_temporal_initial"),
        case("selection_layer_bar_month"),
        case("interactive_layered_crossfilter"),
        case("interactive_seattle_weather"),
        case("concat_marginal_histograms"),
        case("joinaggregate_movie_rating"),
        case("joinaggregate_text_color_contrast"),
        case("cumulative_running_window"),
        case("point_bubble"),
        case("circle_natural_disasters"),
        case("circle_bubble_health_income"),
        case("line_color_stocks"),
        case("line_slope_barley"),
        case("connected_scatterplot"),
        case("layer_line_co2_concentration"),
        case("window_rank_matches"),
        case("circle_github_punchcard"),
        case("rect_lasagna"),
        case("rect_heatmap_weather"),
        case("layer_line_rolling_mean_point_raw"),
        case("layer_histogram_global_mean"),
    )]
    fn test_image_comparison(spec_name: &str) {
        println!("spec_name: {}", spec_name);
        TOKIO_RUNTIME.block_on(check_spec_sequence_from_files(spec_name));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[rustfmt::skip]  // Rust format breaks the rstest macro use below
#[cfg(test)]
mod test_image_comparison_timeunit {
    use super::*;
    use itertools::Itertools;
    use vegafusion_core::spec::transform::timeunit::{TimeUnitTimeZoneSpec, TimeUnitUnitSpec};
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest]
    fn test_image_comparison(
        #[values(
            vec![TimeUnitUnitSpec::Year],
            vec![TimeUnitUnitSpec::Quarter],
            vec![TimeUnitUnitSpec::Month],
            vec![TimeUnitUnitSpec::Week],
            vec![TimeUnitUnitSpec::Date],
            vec![TimeUnitUnitSpec::Day],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Quarter],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Month],
            vec![TimeUnitUnitSpec::Year, TimeUnitUnitSpec::Week],
        )]
        units: Vec<TimeUnitUnitSpec>,

        #[values(
            TimeUnitTimeZoneSpec::Utc,
            TimeUnitTimeZoneSpec::Local,
        )]
        timezone: TimeUnitTimeZoneSpec,

        #[values(
            "bar_month_temporal_initial_parameterize",
            "stacked_bar_weather_timeunit_parameterize"
        )]
        spec_name: &str,
    ) {
        // Load spec
        let mut full_spec = load_spec(spec_name);

        // Load updates
        let full_updates = load_updates(spec_name);

        // Load expected watch plan
        let watch_plan = load_expected_watch_plan(spec_name);

        // Modify transform spec
        let timeunit_tx = full_spec
            .data
            .get_mut(0)
            .unwrap()
            .transform
            .get_mut(0)
            .unwrap();
        if let TransformSpec::Timeunit(timeunit_tx) = timeunit_tx {
            timeunit_tx.units = Some(units.clone());
            timeunit_tx.timezone = Some(timezone.clone());
        } else {
            panic!("Unexpected transform")
        }

        // Build name for saved images
        let units_str = units
            .iter()
            .map(|unit| {
                let s = serde_json::to_string(unit).unwrap();
                s.trim_matches('"').to_string()
            })
            .join("_");
        let timezone_str = serde_json::to_string(&timezone)
            .unwrap()
            .trim_matches('"')
            .to_string();
        let output_name = format!("{}_timeunit_{}_{}", spec_name, units_str, timezone_str);

        TOKIO_RUNTIME.block_on(check_spec_sequence(
            full_spec,
            full_updates,
            watch_plan,
            &output_name,
        ));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_image_comparison_window {
    use super::*;
    use itertools::Itertools;
    use vegafusion_core::spec::transform::timeunit::{TimeUnitTimeZoneSpec, TimeUnitUnitSpec};
    use vegafusion_core::spec::transform::window::WindowTransformOpSpec;
    use vegafusion_core::spec::transform::TransformSpec;

    #[rstest]
    fn test_image_comparison(
        #[values(
            "count",
            "max",
            "min",
            "mean",
            "average",
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "first_value",
            "last_value"
        )]
        op_name: &str,

        #[values("cumulative_running_window")] spec_name: &str,
    ) {
        // Load spec
        let mut full_spec = load_spec(spec_name);

        // Load updates
        let full_updates = load_updates(spec_name);

        // Load expected watch plan
        let watch_plan = load_expected_watch_plan(spec_name);

        // Window
        let window_tx = full_spec
            .data
            .get_mut(0)
            .unwrap()
            .transform
            .get_mut(2)
            .unwrap();

        if let TransformSpec::Window(window_tx) = window_tx {
            let op: WindowTransformOpSpec =
                serde_json::from_str(&format!("\"{}\"", op_name)).unwrap();
            window_tx.ops = vec![op];
        } else {
            panic!("Unexpected transform")
        }

        // Build name for saved images
        let output_name = format!("{}_{}", spec_name, op_name);

        TOKIO_RUNTIME.block_on(check_spec_sequence(
            full_spec,
            full_updates,
            watch_plan,
            &output_name,
        ));
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

fn load_spec(spec_name: &str) -> ChartSpec {
    // Load spec
    let spec_path = format!(
        "{}/tests/specs/sequence/{}/spec.json",
        crate_dir(),
        spec_name
    );
    let spec_str = fs::read_to_string(spec_path).unwrap();
    serde_json::from_str(&spec_str).unwrap()
}

fn load_updates(spec_name: &str) -> Vec<ExportUpdateBatch> {
    let updates_path = format!(
        "{}/tests/specs/sequence/{}/updates.json",
        crate_dir(),
        spec_name
    );
    let updates_str = fs::read_to_string(updates_path).unwrap();
    serde_json::from_str(&updates_str).unwrap()
}

fn load_expected_watch_plan(spec_name: &str) -> WatchPlan {
    let watch_plan_path = format!(
        "{}/tests/specs/sequence/{}/watch_plan.json",
        crate_dir(),
        spec_name
    );
    let watch_plan_str = fs::read_to_string(watch_plan_path).unwrap();
    serde_json::from_str(&watch_plan_str).unwrap()
}

async fn check_spec_sequence_from_files(spec_name: &str) {
    // Load spec
    let full_spec = load_spec(spec_name);

    // Load updates
    let full_updates = load_updates(spec_name);

    // Load expected watch plan
    let watch_plan = load_expected_watch_plan(spec_name);

    check_spec_sequence(full_spec, full_updates, watch_plan, spec_name).await
}

async fn check_spec_sequence(
    full_spec: ChartSpec,
    full_updates: Vec<ExportUpdateBatch>,
    watch_plan: WatchPlan,
    spec_name: &str,
) {
    // Initialize runtime
    let vegajs_runtime = vegajs_runtime();

    // Perform client/server planning
    let mut task_scope = full_spec.to_task_scope().unwrap();
    let mut client_spec = full_spec.clone();
    let mut server_spec = extract_server_data(&mut client_spec, &mut task_scope).unwrap();
    let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut client_spec).unwrap();

    println!(
        "client_spec: {}",
        serde_json::to_string_pretty(&client_spec).unwrap()
    );
    println!(
        "server_spec: {}",
        serde_json::to_string_pretty(&server_spec).unwrap()
    );
    println!("comm_plan: {:#?}", comm_plan);

    // Build task graph
    let tasks = server_spec.to_tasks().unwrap();
    let mut task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let task_graph_mapping = task_graph.build_mapping();

    // Collect watch variables and node indices
    let watch_vars = comm_plan.server_to_client.clone();
    let watch_indices: Vec<_> = watch_vars
        .iter()
        .map(|var| task_graph_mapping.get(var).unwrap())
        .collect();

    // Initialize task graph runtime
    let runtime = TaskGraphRuntime::new(10);

    // Extract the initial values of all of the variables that should be sent from the
    // server to the client

    // println!("comm_plan: {:#?}", comm_plan);

    let mut init = Vec::new();
    for var in &comm_plan.server_to_client {
        let node_index = task_graph_mapping.get(var).unwrap();
        let value = runtime
            .get_node_value(Arc::new(task_graph.clone()), node_index)
            .await
            .expect("Failed to get node value");

        init.push(ExportUpdate {
            namespace: ExportUpdateNamespace::try_from(var.0.namespace()).unwrap(),
            name: var.0.name.clone(),
            scope: var.1.clone(),
            value: value.to_json().unwrap(),
        });
    }

    // println!("init: {:#?}", init);

    // Build watches for all of the variables that should be sent from the client to the
    // server
    let watches: Vec<_> = comm_plan
        .client_to_server
        .iter()
        .map(|t| Watch::try_from(t.clone()).unwrap())
        .collect();

    // Export sequence with full spec
    let export_sequence_results = vegajs_runtime
        .export_spec_sequence(
            &full_spec,
            ExportImageFormat::Png,
            Vec::new(),
            full_updates.clone(),
            watches,
        )
        .unwrap();

    // Save exported PNGs
    for (i, (export_image, _)) in export_sequence_results.iter().enumerate() {
        export_image
            .save(
                &format!("{}/tests/output/{}_full{}", crate_dir(), spec_name, i),
                true,
            )
            .unwrap();
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
            let value = runtime
                .get_node_value(Arc::new(task_graph.clone()), node_index)
                .await
                .unwrap();

            server_to_client_value_batch.insert(var.clone(), value);
        }

        server_to_client_value_batches.push(server_to_client_value_batch);
    }

    // Merge the original updates with the original batch updates
    let planned_spec_updates: Vec<_> = full_updates
        .iter()
        .zip(server_to_client_value_batches)
        .map(|(full_export_updates, server_to_client_values)| {
            let server_to_clients_export_updates: Vec<_> = server_to_client_values
                .iter()
                .map(|(scoped_var, value)| {
                    let json_value = match value {
                        TaskValue::Scalar(value) => value.to_json().unwrap(),
                        TaskValue::Table(value) => value.to_json(),
                    };
                    ExportUpdate {
                        namespace: ExportUpdateNamespace::try_from(scoped_var.0.namespace())
                            .unwrap(),
                        name: scoped_var.0.name.clone(),
                        scope: scoped_var.1.clone(),
                        value: json_value,
                    }
                })
                .collect();

            let mut total_updates = Vec::new();

            total_updates.extend(server_to_clients_export_updates);
            total_updates.extend(full_export_updates.clone());

            // export_updates
            total_updates
        })
        .collect();

    // Export the planned client spec with updates from task graph

    // Compare exported images
    for (i, server_img) in vegajs_runtime
        .export_spec_sequence(
            &client_spec,
            ExportImageFormat::Png,
            init,
            planned_spec_updates,
            Default::default(),
        )
        .unwrap()
        .into_iter()
        .map(|(img, _)| img)
        .enumerate()
    {
        server_img.save(
            &format!("{}/tests/output/{}_planned{}", crate_dir(), spec_name, i),
            true,
        );
        let (full_img, _) = &export_sequence_results[i];

        let (difference, diff_img) = full_img.compare(&server_img).unwrap();
        if difference > 1e-3 {
            println!("difference: {}", difference);
            if let Some(diff_img) = diff_img {
                let diff_path = format!("{}/tests/output/{}_diff{}.png", crate_dir(), spec_name, i);
                fs::write(&diff_path, diff_img).unwrap();
                assert!(
                    false,
                    "Found difference in exported images.\nDiff written to {}",
                    diff_path
                )
            }
        }
    }

    // Check for expected comm plan
    assert_eq!(watch_plan, WatchPlan::from(comm_plan))
}
