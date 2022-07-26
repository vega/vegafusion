use std::fs;
use vegafusion_core::planning::plan::SpecPlan;
use vegafusion_core::planning::watch::ExportUpdateBatch;
use vegafusion_core::proto::gen::services::query_request::Request;
use vegafusion_core::proto::gen::services::{QueryRequest, QueryResult};
use vegafusion_core::proto::gen::tasks::{TaskGraph, TaskGraphValueRequest, TzConfig, Variable};
use vegafusion_core::spec::chart::ChartSpec;

use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

fn load_spec(spec_name: &str) -> ChartSpec {
    // Load spec
    let spec_path = format!("{}/benches/specs/{}.vg.json", crate_dir(), spec_name);
    let spec_str = fs::read_to_string(spec_path).unwrap();
    serde_json::from_str(&spec_str).unwrap()
}

fn load_updates(spec_name: &str) -> Vec<ExportUpdateBatch> {
    let updates_path = format!("{}/benches/specs/{}.updates.json", crate_dir(), spec_name);
    let updates_path = std::path::Path::new(&updates_path);

    if updates_path.exists() {
        let updates_str = fs::read_to_string(updates_path).unwrap();
        serde_json::from_str(&updates_str).unwrap()
    } else {
        Vec::new()
    }
}

async fn eval_spec_sequence_from_files(spec_name: &str) {
    // Load spec
    let full_spec = load_spec(spec_name);

    // Load updates
    let full_updates = load_updates(spec_name);
    eval_spec_sequence(full_spec, full_updates).await
}

async fn eval_spec_get_variable(full_spec: ChartSpec, var: &ScopedVariable) -> QueryResult {
    let tz_config = TzConfig {
        local_tz: "America/New_York".to_string(),
        default_input_tz: None,
    };
    let spec_plan = SpecPlan::try_new(&full_spec, &Default::default()).unwrap();
    let task_scope = spec_plan.server_spec.to_task_scope().unwrap();
    let tasks = spec_plan
        .server_spec
        .to_tasks(&tz_config, &Default::default())
        .unwrap();
    let task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let task_graph_mapping = task_graph.build_mapping();

    // Initialize task graph runtime
    let runtime = TaskGraphRuntime::new(Some(64), None);

    let node_index = task_graph_mapping.get(var).unwrap();

    // Make Query request
    let request = QueryRequest {
        request: Some(Request::TaskGraphValues(TaskGraphValueRequest {
            task_graph: Some(task_graph.clone()),
            indices: vec![node_index.clone()],
        })),
    };

    runtime.query_request(request).await.unwrap()
}

async fn eval_spec_sequence(full_spec: ChartSpec, full_updates: Vec<ExportUpdateBatch>) {
    let tz_config = TzConfig {
        local_tz: "America/New_York".to_string(),
        default_input_tz: None,
    };
    let spec_plan = SpecPlan::try_new(&full_spec, &Default::default()).unwrap();
    let task_scope = spec_plan.server_spec.to_task_scope().unwrap();
    let comm_plan = spec_plan.comm_plan.clone();

    // println!(
    //     "client_spec: {}",
    //     serde_json::to_string_pretty(&spec_plan.client_spec).unwrap()
    // );
    // println!(
    //     "server_spec: {}",
    //     serde_json::to_string_pretty(&spec_plan.server_spec).unwrap()
    // );
    //
    // println!(
    //     "comm_plan:\n---\n{}\n---",
    //     serde_json::to_string_pretty(&WatchPlan::from(spec_plan.comm_plan.clone())).unwrap()
    // );

    // Build task graph
    let tasks = spec_plan
        .server_spec
        .to_tasks(&tz_config, &Default::default())
        .unwrap();
    let mut task_graph = TaskGraph::new(tasks, &task_scope).unwrap();
    let task_graph_mapping = task_graph.build_mapping();

    // Initialize task graph runtime
    let runtime = TaskGraphRuntime::new(Some(64), None);

    // Get initial values
    let mut query_indices = Vec::new();
    for var in comm_plan.server_to_client {
        let node_index = task_graph_mapping.get(&var).unwrap();
        query_indices.push(node_index.clone());
    }
    // Make Query request
    let request = QueryRequest {
        request: Some(Request::TaskGraphValues(TaskGraphValueRequest {
            task_graph: Some(task_graph.clone()),
            indices: query_indices,
        })),
    };
    let _response = runtime.query_request(request).await.unwrap();

    // Get update values
    for update_batch in full_updates {
        let mut query_indices = Vec::new();
        for update in update_batch {
            let var = update.to_scoped_var();
            let value = update.to_task_value();
            let node_index = task_graph_mapping.get(&var).unwrap().node_index;
            query_indices.extend(task_graph.update_value(node_index as usize, value).unwrap());
        }

        // Make Query request
        let request = QueryRequest {
            request: Some(Request::TaskGraphValues(TaskGraphValueRequest {
                task_graph: Some(task_graph.clone()),
                indices: query_indices,
            })),
        };
        let _response = runtime.query_request(request).await.unwrap();
    }
}

use criterion::{criterion_group, criterion_main, Criterion};
use vegafusion_core::task_graph::graph::ScopedVariable;

fn make_tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn flights_crossfilter(c: &mut Criterion) {
    // Load spec
    let spec_name = "flights_crossfilter";
    let full_spec = load_spec(spec_name);
    let full_updates = load_updates(spec_name);

    let tokio_runtime = make_tokio_runtime();

    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_sequence(full_spec.clone(), full_updates.clone()))
    });
}

pub fn flights_crossfilter_local_time(c: &mut Criterion) {
    // Initialize runtime
    let tokio_runtime = make_tokio_runtime();

    // Load spec
    let spec_name = "flights_crossfilter_local_time";
    let full_spec = load_spec(spec_name);
    let full_updates = load_updates(spec_name);

    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_sequence(full_spec.clone(), full_updates.clone()))
    });
}

pub fn load_flights_crossfilter_data_local(c: &mut Criterion) {
    let tokio_runtime = make_tokio_runtime();
    let spec_name = "load_flights_crossfilter_data_local";
    let full_spec = load_spec(spec_name);
    let var: ScopedVariable = (Variable::new_data("source_0"), Vec::new());
    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_get_variable(full_spec.clone(), &var))
    });
}

pub fn load_flights_crossfilter_data_utc(c: &mut Criterion) {
    let tokio_runtime = make_tokio_runtime();
    let spec_name = "load_flights_crossfilter_data_utc";
    let full_spec = load_spec(spec_name);
    let var: ScopedVariable = (Variable::new_data("source_0"), Vec::new());
    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_get_variable(full_spec.clone(), &var))
    });
}

pub fn load_flights_crossfilter_data_200k_utc(c: &mut Criterion) {
    let mut group = c.benchmark_group("small-sample");
    group.sample_size(10);

    let tokio_runtime = make_tokio_runtime();
    let spec_name = "load_flights_crossfilter_data_200k_utc";
    let full_spec = load_spec(spec_name);
    let var: ScopedVariable = (Variable::new_data("source_0"), Vec::new());
    group.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_get_variable(full_spec.clone(), &var))
    });
}

pub fn stacked_bar_weather_year_local(c: &mut Criterion) {
    // Initialize runtime
    let tokio_runtime = make_tokio_runtime();

    // Load spec
    let spec_name = "stacked_bar_weather_year_local";
    let full_spec = load_spec(spec_name);
    let full_updates = Vec::new();

    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_sequence(full_spec.clone(), full_updates.clone()))
    });
}

pub fn stacked_bar_weather_year_utc(c: &mut Criterion) {
    // Initialize runtime
    let tokio_runtime = make_tokio_runtime();

    // Load spec
    let spec_name = "stacked_bar_weather_year_utc";
    let full_spec = load_spec(spec_name);
    let full_updates = Vec::new();

    c.bench_function(spec_name, |b| {
        b.to_async(&tokio_runtime)
            .iter(|| eval_spec_sequence(full_spec.clone(), full_updates.clone()))
    });
}

criterion_group!(
    benches,
    flights_crossfilter,
    flights_crossfilter_local_time,
    load_flights_crossfilter_data_local,
    load_flights_crossfilter_data_utc,
    load_flights_crossfilter_data_200k_utc,
    stacked_bar_weather_year_local,
    stacked_bar_weather_year_utc,
);
criterion_main!(benches);
