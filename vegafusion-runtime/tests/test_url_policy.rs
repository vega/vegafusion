use async_trait::async_trait;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use vegafusion_common::arrow::array::{ArrayRef, Float64Array};
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::error::Result;
use vegafusion_core::proto::gen::tasks::{TaskGraph, TzConfig, Variable};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::data::pipeline::BaseUrlSetting;
use vegafusion_runtime::data::plan_resolver::{PlanResolver, ResolutionResult};
use vegafusion_runtime::task_graph::runtime::{VegaFusionRuntime, VegaFusionRuntimeOpts};

fn write_json_rows(dir: &Path, name: &str, values: &[f64]) -> PathBuf {
    let path = dir.join(name);
    let rows: Vec<_> = values.iter().map(|value| json!({ "x": value })).collect();
    fs::write(&path, serde_json::to_string(&rows).unwrap()).unwrap();
    path
}

fn extent_spec(url: serde_json::Value) -> ChartSpec {
    serde_json::from_value(json!({
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": "source",
                "url": url,
                "format": {"type": "json"},
            },
            {
                "name": "derived",
                "source": "source",
                "transform": [
                    {
                        "type": "extent",
                        "signal": "my_extent",
                        "field": "x",
                    }
                ],
            }
        ]
    }))
    .unwrap()
}

fn extent_spec_with_url_signal(signal_url: &str) -> ChartSpec {
    serde_json::from_value(json!({
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "signals": [
            {
                "name": "url",
                "value": signal_url,
            }
        ],
        "data": [
            {
                "name": "source",
                "url": {"signal": "url"},
                "format": {"type": "json"},
            },
            {
                "name": "derived",
                "source": "source",
                "transform": [
                    {
                        "type": "extent",
                        "signal": "my_extent",
                        "field": "x",
                    }
                ],
            }
        ]
    }))
    .unwrap()
}

async fn query_extent(runtime: &VegaFusionRuntime, spec: &ChartSpec) -> Result<[f64; 2]> {
    let tz_config = TzConfig {
        local_tz: "UTC".to_string(),
        default_input_tz: None,
    };
    let task_scope = spec.to_task_scope().unwrap();
    let tasks = spec.to_tasks(&tz_config, &Default::default()).unwrap();
    let graph = Arc::new(TaskGraph::new(tasks, &task_scope).unwrap());
    let mapping = graph.build_mapping();
    let node = mapping
        .get(&(Variable::new_signal("my_extent"), Vec::new()))
        .cloned()
        .unwrap();
    let value = runtime
        .get_node_value(graph, &node, Default::default())
        .await?;
    value.as_scalar()?.to_f64x2()
}

struct CustomSchemeResolver;

#[async_trait]
impl PlanResolver for CustomSchemeResolver {
    fn name(&self) -> &str {
        "custom_scheme_resolver"
    }

    async fn scan_url(
        &self,
        parsed_url: &vegafusion_core::runtime::ParsedUrl,
    ) -> Result<Option<LogicalPlan>> {
        if parsed_url.scheme != "custom" {
            return Ok(None);
        }

        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])) as ArrayRef,
        )])
        .unwrap();
        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let plan = LogicalPlanBuilder::scan(
            "custom_table",
            provider_as_source(Arc::new(mem_table)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        Ok(Some(plan))
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        Ok(ResolutionResult::Plan(plan))
    }
}

fn tempdir_str(tempdir: &TempDir) -> String {
    tempdir.path().to_str().unwrap().to_string()
}

#[tokio::test]
async fn test_relative_url_resolves_against_base_url_and_allowlist() {
    let tempdir = tempfile::tempdir().unwrap();
    write_json_rows(tempdir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let runtime = VegaFusionRuntime::new(VegaFusionRuntimeOpts {
        base_url: BaseUrlSetting::Custom(tempdir_str(&tempdir)),
        allowed_base_urls: Some(vec![tempdir_str(&tempdir)]),
        ..Default::default()
    })
    .unwrap();

    let extent = query_extent(&runtime, &extent_spec(json!("data.json")))
        .await
        .unwrap();
    assert_eq!(extent, [1.0, 3.0]);
}

#[tokio::test]
async fn test_relative_url_fails_when_base_url_disabled() {
    let tempdir = tempfile::tempdir().unwrap();
    write_json_rows(tempdir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let runtime = VegaFusionRuntime::new(VegaFusionRuntimeOpts {
        base_url: BaseUrlSetting::Disabled,
        ..Default::default()
    })
    .unwrap();

    let err = query_extent(&runtime, &extent_spec(json!("data.json")))
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("Relative URL with no base_url configured"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn test_allowed_base_urls_block_local_file_access() {
    let allowed_dir = tempfile::tempdir().unwrap();
    let blocked_dir = tempfile::tempdir().unwrap();
    write_json_rows(blocked_dir.path(), "data.json", &[1.0, 2.0, 3.0]);

    let runtime = VegaFusionRuntime::new(VegaFusionRuntimeOpts {
        base_url: BaseUrlSetting::Custom(tempdir_str(&blocked_dir)),
        allowed_base_urls: Some(vec![tempdir_str(&allowed_dir)]),
        ..Default::default()
    })
    .unwrap();

    let err = query_extent(&runtime, &extent_spec(json!("data.json")))
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("blocked by allowed_base_urls"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn test_allowed_base_urls_gate_custom_scheme_resolvers() {
    let runtime = VegaFusionRuntime::new(VegaFusionRuntimeOpts {
        plan_resolvers: vec![Arc::new(CustomSchemeResolver)],
        allowed_base_urls: Some(vec!["custom://allowed-host/".to_string()]),
        ..Default::default()
    })
    .unwrap();

    let allowed_extent = query_extent(
        &runtime,
        &extent_spec(json!("custom://allowed-host/warehouse/table")),
    )
    .await
    .unwrap();
    assert_eq!(allowed_extent, [10.0, 30.0]);

    let err = query_extent(
        &runtime,
        &extent_spec(json!("custom://blocked-host/warehouse/table")),
    )
    .await
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("blocked by allowed_base_urls"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn test_signal_updated_urls_are_revalidated_against_policy() {
    let runtime = VegaFusionRuntime::new(VegaFusionRuntimeOpts {
        plan_resolvers: vec![Arc::new(CustomSchemeResolver)],
        allowed_base_urls: Some(vec!["custom://allowed-host/".to_string()]),
        ..Default::default()
    })
    .unwrap();

    let allowed_extent = query_extent(
        &runtime,
        &extent_spec_with_url_signal("custom://allowed-host/warehouse/table"),
    )
    .await
    .unwrap();
    assert_eq!(allowed_extent, [10.0, 30.0]);

    let err = query_extent(
        &runtime,
        &extent_spec_with_url_signal("custom://blocked-host/warehouse/table"),
    )
    .await
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("blocked by allowed_base_urls"),
        "unexpected error: {message}"
    );
}
