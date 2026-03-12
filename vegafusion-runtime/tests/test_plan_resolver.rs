use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlan as DFLogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use vegafusion_common::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
use vegafusion_core::proto::gen::tasks::ResolverCapabilities;
use vegafusion_core::runtime::{ParsedUrl, PlanResolver, ResolutionResult, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::data::external_table::ExternalTableProvider;
use vegafusion_runtime::data::pipeline::ResolverPipeline;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

#[derive(Clone, Debug)]
struct ResolverEvent {
    resolver_id: String,
    table_names: Vec<String>,
    has_external_scan: bool,
}

#[derive(Clone)]
enum ResolverBehavior {
    PassThroughPlan,
    RewriteExternalToMemTable,
    ReturnTable(VegaFusionTable),
    Fail(&'static str),
}

#[derive(Clone)]
struct ScriptedResolver {
    id: String,
    call_count: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<ResolverEvent>>>,
    behavior: ResolverBehavior,
    movies_table: Arc<dyn TableProvider>,
}

impl ScriptedResolver {
    fn new(
        id: impl Into<String>,
        behavior: ResolverBehavior,
        events: Arc<Mutex<Vec<ResolverEvent>>>,
    ) -> Self {
        let movies_table = create_movies_table();
        let schema = movies_table.schema.clone();
        let batches = movies_table.batches.clone();
        let mem_table =
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()) as Arc<dyn TableProvider>;

        Self {
            id: id.into(),
            call_count: Arc::new(AtomicUsize::new(0)),
            events,
            behavior,
            movies_table: mem_table,
        }
    }

    fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

struct TableRewriter {
    movies_table: Arc<dyn TableProvider>,
}

impl TreeNodeRewriter for TableRewriter {
    type Node = DFLogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let DFLogicalPlan::TableScan(scan) = &node {
            // Verify that the source is actually our ExternalTableProvider
            if datafusion::datasource::source_as_provider(&scan.source)
                .ok()
                .and_then(|p| {
                    p.as_any()
                        .downcast_ref::<ExternalTableProvider>()
                        .map(|_| ())
                })
                .is_some()
            {
                let new_scan = DFLogicalPlan::TableScan(datafusion_expr::TableScan {
                    table_name: scan.table_name.clone(),
                    source: provider_as_source(self.movies_table.clone()),
                    projection: scan.projection.clone(),
                    projected_schema: scan.projected_schema.clone(),
                    filters: scan.filters.clone(),
                    fetch: scan.fetch,
                });
                return Ok(Transformed::yes(new_scan));
            }
        }
        Ok(Transformed::no(node))
    }
}

#[async_trait]
impl PlanResolver for ScriptedResolver {
    fn name(&self) -> &str {
        &self.id
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        let event = ResolverEvent {
            resolver_id: self.id.clone(),
            table_names: collect_scan_table_names(&plan),
            has_external_scan: has_external_table_scans(&plan),
        };
        self.events.lock().unwrap().push(event);

        match &self.behavior {
            ResolverBehavior::PassThroughPlan => Ok(ResolutionResult::Plan(plan)),
            ResolverBehavior::RewriteExternalToMemTable => {
                let mut rewriter = TableRewriter {
                    movies_table: self.movies_table.clone(),
                };
                let rewritten_plan = plan.rewrite(&mut rewriter).unwrap().data;
                Ok(ResolutionResult::Plan(rewritten_plan))
            }
            ResolverBehavior::ReturnTable(table) => Ok(ResolutionResult::Table(table.clone())),
            ResolverBehavior::Fail(msg) => Err(VegaFusionError::internal(*msg)),
        }
    }
}

fn collect_scan_table_names(plan: &LogicalPlan) -> Vec<String> {
    let mut table_names = Vec::new();
    let _ = plan.apply(|node| {
        if let DFLogicalPlan::TableScan(scan) = node {
            table_names.push(scan.table_name.table().to_string());
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    table_names
}

fn has_external_table_scans(plan: &LogicalPlan) -> bool {
    let mut has_external = false;
    let _ = plan.apply(|node| {
        if let DFLogicalPlan::TableScan(scan) = node {
            let is_external = datafusion::datasource::source_as_provider(&scan.source)
                .ok()
                .map(|provider| {
                    provider
                        .as_any()
                        .downcast_ref::<ExternalTableProvider>()
                        .is_some()
                })
                .unwrap_or(false);
            if is_external {
                has_external = true;
                return Ok(datafusion_common::tree_node::TreeNodeRecursion::Stop);
            }
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    has_external
}

fn table_row_count(table: &VegaFusionTable) -> usize {
    table.batches.iter().map(|batch| batch.num_rows()).sum()
}

fn build_external_scan_plan(table_name: &str) -> LogicalPlan {
    let schema = get_movies_schema();
    let provider = Arc::new(ExternalTableProvider::new(
        schema,
        Some("test".to_string()),
        serde_json::Value::Null,
    ));
    let table_source = provider_as_source(provider);
    LogicalPlanBuilder::scan(table_name, table_source, None)
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_spec() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new(
        "pre_transform_spec",
        ResolverBehavior::RewriteExternalToMemTable,
        events,
    );
    let resolver_clone = resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_extract() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new(
        "pre_transform_extract",
        ResolverBehavior::RewriteExternalToMemTable,
        events,
    );
    let resolver_clone = resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();

    let (_transformed_spec, _datasets, warnings) = runtime
        .pre_transform_extract(
            &spec,
            &inline_datasets,
            &vegafusion_core::proto::gen::pretransform::PreTransformExtractOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                extract_threshold: 100,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_values() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new(
        "pre_transform_values",
        ResolverBehavior::RewriteExternalToMemTable,
        events,
    );
    let resolver_clone = resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();

    let variables = vec![(
        vegafusion_core::proto::gen::tasks::Variable {
            namespace: vegafusion_core::proto::gen::tasks::VariableNamespace::Data as i32,
            name: "source_0".to_string(),
        },
        vec![],
    )];

    let (values, warnings) = runtime
        .pre_transform_values(
            &spec,
            &variables,
            &inline_datasets,
            &vegafusion_core::proto::gen::pretransform::PreTransformValuesOpts {
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(values.len(), 1);

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

// This remains an API-level smoke test for resolver wiring on a multi-step transform chain.
#[tokio::test]
async fn test_bin_transform_uses_custom_executor() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new(
        "bin_transform",
        ResolverBehavior::RewriteExternalToMemTable,
        events,
    );
    let resolver_clone = resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec_str = r#"{
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 200,
        "height": 200,
        "padding": 5,
        "data": [
            {
                "name": "source_0",
                "url": "table://movies",
                "transform": [
                    {
                        "type": "extent",
                        "field": "imdb_rating",
                        "signal": "bin_maxbins_10_imdb_rating_extent"
                    },
                    {
                        "type": "bin",
                        "field": "imdb_rating",
                        "as": [
                            "bin_maxbins_10_imdb_rating",
                            "bin_maxbins_10_imdb_rating_end"
                        ],
                        "signal": "bin_maxbins_10_imdb_rating_bins",
                        "extent": {"signal": "bin_maxbins_10_imdb_rating_extent"},
                        "maxbins": 10
                    },
                    {
                        "type": "aggregate",
                        "groupby": [
                            "bin_maxbins_10_imdb_rating",
                            "bin_maxbins_10_imdb_rating_end"
                        ],
                        "ops": ["count"],
                        "fields": [null],
                        "as": ["__count"]
                    }
                ]
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source_0"},
                "encode": {
                    "update": {
                        "x": {"scale": "xscale", "field": "bin_maxbins_10_imdb_rating"},
                        "x2": {"scale": "xscale", "field": "bin_maxbins_10_imdb_rating_end"},
                        "y": {"scale": "yscale", "field": "__count"},
                        "y2": {"scale": "yscale", "value": 0}
                    }
                }
            }
        ],
        "scales": [
            {
                "name": "xscale",
                "type": "linear",
                "domain": {
                    "signal": "[bin_maxbins_10_imdb_rating_bins.start, bin_maxbins_10_imdb_rating_bins.stop]"
                },
                "range": "width",
                "bins": {"signal": "bin_maxbins_10_imdb_rating_bins"}
            },
            {
                "name": "yscale",
                "type": "linear",
                "domain": {"data": "source_0", "field": "__count"},
                "range": "height",
                "nice": true
            }
        ]
    }"#;

    let spec: ChartSpec = serde_json::from_str(spec_str).unwrap();
    let inline_datasets = get_inline_datasets();

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = resolver_clone.get_call_count();

    assert!(call_count >= 1, "Custom resolver should have been called");
}

#[tokio::test]
async fn test_mixed_data_only_executes_plans() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new(
        "mixed_data",
        ResolverBehavior::RewriteExternalToMemTable,
        events.clone(),
    );
    let resolver_clone = resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec_str = r#"{
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 400,
        "height": 200,
        "padding": 5,
        "data": [
            {
                "name": "source_table",
                "url": "table://movies_table",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["genre"],
                        "ops": ["count"],
                        "fields": [null],
                        "as": ["count_from_table"]
                    }
                ]
            },
            {
                "name": "source_plan",
                "url": "table://movies_plan",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["genre"],
                        "ops": ["count"],
                        "fields": [null],
                        "as": ["count_from_plan"]
                    }
                ]
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source_table"}
            },
            {
                "type": "rect",
                "from": {"data": "source_plan"}
            }
        ]
    }"#;

    let spec: ChartSpec = serde_json::from_str(spec_str).unwrap();

    let movies_table = create_movies_table();
    let movies_plan = build_external_scan_plan("plan_only_source");

    let mut inline_datasets = std::collections::HashMap::new();
    inline_datasets.insert(
        "movies_table".to_string(),
        VegaFusionDataset::from_table(movies_table, None).unwrap(),
    );
    inline_datasets.insert(
        "movies_plan".to_string(),
        VegaFusionDataset::from_plan(movies_plan),
    );

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    assert!(
        resolver_clone.get_call_count() > 0,
        "Custom resolver should have been called for plan-backed datasets"
    );

    let observed_events = events.lock().unwrap().clone();
    assert!(
        observed_events.iter().all(|event| event.has_external_scan),
        "Expected only external plan scans in resolver inputs"
    );
    let observed_table_names: Vec<String> = observed_events
        .iter()
        .flat_map(|event| event.table_names.clone())
        .collect();
    assert!(
        observed_table_names
            .iter()
            .any(|table| table == "plan_only_source"),
        "Expected resolver to observe the plan-backed source table"
    );
    assert!(
        observed_table_names
            .iter()
            .all(|table| table != "movies_table"),
        "Resolver should not run for table-backed datasets"
    );
}

#[tokio::test]
async fn test_execute_plan_pipeline_chains_resolvers_in_order() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let rewrite_resolver = ScriptedResolver::new(
        "rewrite",
        ResolverBehavior::RewriteExternalToMemTable,
        events.clone(),
    );
    let inspect_resolver =
        ScriptedResolver::new("inspect", ResolverBehavior::PassThroughPlan, events.clone());

    let resolvers: Vec<Arc<dyn PlanResolver>> = vec![
        Arc::new(rewrite_resolver.clone()),
        Arc::new(inspect_resolver.clone()),
    ];
    let plan = build_external_scan_plan("movies_chain");
    let ctx = datafusion::prelude::SessionContext::new();

    let pipeline = ResolverPipeline::new(resolvers, Arc::new(ctx));
    let table = pipeline.resolve(plan).await.unwrap();
    assert_eq!(table_row_count(&table), 10);
    assert_eq!(rewrite_resolver.get_call_count(), 1);
    assert_eq!(inspect_resolver.get_call_count(), 1);

    let observed_events = events.lock().unwrap().clone();
    assert_eq!(observed_events.len(), 2);
    assert_eq!(observed_events[0].resolver_id, "rewrite");
    assert!(observed_events[0].has_external_scan);
    assert_eq!(observed_events[1].resolver_id, "inspect");
    assert!(
        !observed_events[1].has_external_scan,
        "Second resolver should see rewritten non-external plan"
    );
}

#[tokio::test]
async fn test_execute_plan_pipeline_short_circuits_after_table_result() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let table_resolver = ScriptedResolver::new(
        "table",
        ResolverBehavior::ReturnTable(create_movies_table()),
        events.clone(),
    );
    let never_called_resolver = ScriptedResolver::new(
        "never_called",
        ResolverBehavior::Fail("second resolver should not have been called"),
        events.clone(),
    );

    let resolvers: Vec<Arc<dyn PlanResolver>> = vec![
        Arc::new(table_resolver.clone()),
        Arc::new(never_called_resolver.clone()),
    ];
    let plan = build_external_scan_plan("movies_short_circuit");
    let ctx = datafusion::prelude::SessionContext::new();

    let pipeline = ResolverPipeline::new(resolvers, Arc::new(ctx));
    let table = pipeline.resolve(plan).await.unwrap();
    assert_eq!(table_row_count(&table), 10);
    assert_eq!(table_resolver.get_call_count(), 1);
    assert_eq!(never_called_resolver.get_call_count(), 0);

    let observed_events = events.lock().unwrap().clone();
    assert_eq!(observed_events.len(), 1);
    assert_eq!(observed_events[0].resolver_id, "table");
}

#[tokio::test]
async fn test_execute_plan_pipeline_fails_if_external_not_resolved() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let passthrough_resolver = ScriptedResolver::new(
        "passthrough",
        ResolverBehavior::PassThroughPlan,
        events.clone(),
    );

    let resolvers: Vec<Arc<dyn PlanResolver>> = vec![Arc::new(passthrough_resolver.clone())];
    let plan = build_external_scan_plan("movies_unresolved");
    let ctx = datafusion::prelude::SessionContext::new();

    let pipeline = ResolverPipeline::new(resolvers, Arc::new(ctx));
    let err = pipeline.resolve(plan).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("cannot be executed directly"),
        "Expected unresolved external-table execution error, got: {msg}"
    );
    assert_eq!(passthrough_resolver.get_call_count(), 1);
}

fn get_simple_spec() -> ChartSpec {
    let spec_str = r#"{
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 400,
        "height": 200,
        "padding": 5,
        "data": [
            {
                "name": "source_0",
                "url": "table://movies",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["genre"],
                        "ops": ["sum", "average", "count"],
                        "fields": ["worldwide_gross", "imdb_rating", null],
                        "as": ["total_gross", "avg_rating", "movie_count"]
                    }
                ]
            }
        ],
        "scales": [
            {
                "name": "xscale",
                "type": "band",
                "domain": {"data": "source_0", "field": "genre"},
                "range": "width",
                "padding": 0.05
            },
            {
                "name": "yscale",
                "domain": {"data": "source_0", "field": "total_gross"},
                "nice": true,
                "range": "height"
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source_0"},
                "encode": {
                    "enter": {
                        "x": {"scale": "xscale", "field": "genre"},
                        "width": {"scale": "xscale", "band": 1},
                        "y": {"scale": "yscale", "field": "total_gross"},
                        "y2": {"scale": "yscale", "value": 0}
                    }
                }
            }
        ]
    }"#;

    serde_json::from_str(spec_str).unwrap()
}

fn get_movies_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("title", DataType::Utf8, false),
        Field::new("genre", DataType::Utf8, false),
        Field::new("director", DataType::Utf8, true),
        Field::new("release_year", DataType::Int64, false),
        Field::new("worldwide_gross", DataType::Int64, false),
        Field::new("production_budget", DataType::Int64, true),
        Field::new("imdb_rating", DataType::Float64, true),
        Field::new("rotten_tomatoes", DataType::Int64, true),
    ]))
}

#[allow(dead_code)]
fn create_movies_table() -> VegaFusionTable {
    let schema = get_movies_schema();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "The Shawshank Redemption",
                "The Dark Knight",
                "Inception",
                "Pulp Fiction",
                "Forrest Gump",
                "The Matrix",
                "Goodfellas",
                "The Silence of the Lambs",
                "Interstellar",
                "The Prestige",
            ])),
            Arc::new(StringArray::from(vec![
                "Drama", "Action", "Action", "Crime", "Drama", "Action", "Crime", "Thriller",
                "Sci-Fi", "Thriller",
            ])),
            Arc::new(StringArray::from(vec![
                Some("Frank Darabont"),
                Some("Christopher Nolan"),
                Some("Christopher Nolan"),
                Some("Quentin Tarantino"),
                Some("Robert Zemeckis"),
                Some("The Wachowskis"),
                Some("Martin Scorsese"),
                Some("Jonathan Demme"),
                Some("Christopher Nolan"),
                Some("Christopher Nolan"),
            ])),
            Arc::new(Int64Array::from(vec![
                1994, 2008, 2010, 1994, 1994, 1999, 1990, 1991, 2014, 2006,
            ])),
            Arc::new(Int64Array::from(vec![
                28341469, 1004558444, 836848102, 213928762, 678226465, 463517383, 46836394,
                272742922, 677471339, 109676311,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(25000000),
                Some(185000000),
                Some(160000000),
                Some(8000000),
                Some(55000000),
                Some(63000000),
                Some(25000000),
                Some(19000000),
                Some(165000000),
                Some(40000000),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(9.3),
                Some(9.0),
                Some(8.8),
                Some(8.9),
                Some(8.8),
                Some(8.7),
                Some(8.7),
                Some(8.6),
                Some(8.6),
                Some(8.5),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(91),
                Some(94),
                Some(87),
                Some(92),
                Some(71),
                Some(88),
                Some(96),
                Some(96),
                Some(72),
                Some(76),
            ])),
        ],
    )
    .unwrap();

    VegaFusionTable::from(batch)
}

fn create_movies_logical_plan() -> LogicalPlan {
    build_external_scan_plan("movies")
}

fn get_inline_datasets() -> std::collections::HashMap<String, VegaFusionDataset> {
    let logical_plan = create_movies_logical_plan();
    let dataset = VegaFusionDataset::from_plan(logical_plan);

    let mut datasets = std::collections::HashMap::new();
    datasets.insert("movies".to_string(), dataset);
    datasets
}

// ── scan_url tests ──

/// A resolver that claims custom:// URLs by returning an ExternalTableProvider plan
struct CustomSchemeScanner {
    schema: Arc<Schema>,
}

#[async_trait]
impl PlanResolver for CustomSchemeScanner {
    fn name(&self) -> &str {
        "custom_scheme_scanner"
    }

    fn capabilities(&self) -> ResolverCapabilities {
        ResolverCapabilities {
            supported_schemes: vec!["custom".to_string()],
            supported_format_types: vec![],
            supported_extensions: vec![],
        }
    }

    async fn scan_url(&self, parsed_url: &ParsedUrl) -> Result<Option<LogicalPlan>> {
        if parsed_url.scheme == "custom" {
            let provider = Arc::new(ExternalTableProvider::new(
                self.schema.clone(),
                Some("custom".to_string()),
                serde_json::json!({"url": parsed_url.url}),
            ));
            let plan = LogicalPlanBuilder::scan("custom_table", provider_as_source(provider), None)
                .unwrap()
                .build()
                .unwrap();
            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        // Rewrite ExternalTableProvider to MemTable for execution
        let movies = create_movies_table();
        let mem_table = Arc::new(
            MemTable::try_new(movies.schema.clone(), vec![movies.batches.clone()]).unwrap(),
        ) as Arc<dyn TableProvider>;
        let mut rewriter = TableRewriter {
            movies_table: mem_table,
        };
        let rewritten = plan.rewrite(&mut rewriter).unwrap().data;
        Ok(ResolutionResult::Plan(rewritten))
    }
}

#[tokio::test]
async fn test_scan_url_custom_scheme_first_wins() {
    let schema = get_movies_schema();
    let scanner = CustomSchemeScanner {
        schema: schema.clone(),
    };

    let ctx = Arc::new(datafusion::prelude::SessionContext::new());
    let pipeline = ResolverPipeline::new(vec![Arc::new(scanner)], ctx);

    let parsed = ParsedUrl {
        url: "custom://mydb/table1".to_string(),
        scheme: "custom".to_string(),
        host: Some("mydb".to_string()),
        path: "/table1".to_string(),
        query_params: vec![],
        extension: None,
        format_type: None,
    };

    let result = pipeline.scan_url(&parsed).await.unwrap();
    assert!(
        result.is_some(),
        "Custom scanner should handle custom:// URLs"
    );
}

#[tokio::test]
async fn test_scan_url_unknown_scheme_falls_through() {
    let ctx = Arc::new(datafusion::prelude::SessionContext::new());
    // Pipeline with only DataFusionResolver (no user resolvers)
    let pipeline = ResolverPipeline::new(vec![], ctx);

    let parsed = ParsedUrl {
        url: "spark://cluster/table1".to_string(),
        scheme: "spark".to_string(),
        host: Some("cluster".to_string()),
        path: "/table1".to_string(),
        query_params: vec![],
        extension: None,
        format_type: None,
    };

    let result = pipeline.scan_url(&parsed).await.unwrap();
    assert!(
        result.is_none(),
        "DataFusionResolver should return None for unknown schemes"
    );
}

#[tokio::test]
async fn test_has_user_resolvers() {
    let ctx = Arc::new(datafusion::prelude::SessionContext::new());

    // No user resolvers
    let pipeline = ResolverPipeline::new(vec![], ctx.clone());
    assert!(!pipeline.has_user_resolvers());

    // With a user resolver
    let scanner = CustomSchemeScanner {
        schema: get_movies_schema(),
    };
    let pipeline = ResolverPipeline::new(vec![Arc::new(scanner)], ctx);
    assert!(pipeline.has_user_resolvers());
}

#[tokio::test]
async fn test_merged_capabilities_includes_custom_resolver() {
    let schema = get_movies_schema();
    let scanner = CustomSchemeScanner { schema };

    let ctx = Arc::new(datafusion::prelude::SessionContext::new());
    let pipeline = ResolverPipeline::new(vec![Arc::new(scanner)], ctx);

    let caps = pipeline.merged_capabilities();
    // DataFusion built-ins
    assert!(caps.supported_schemes.contains("http"));
    assert!(caps.supported_schemes.contains("file"));
    assert!(caps.supported_format_types.contains("csv"));
    // Custom resolver additions
    assert!(caps.supported_schemes.contains("custom"));
    // url_supported checks
    assert!(caps.url_supported("custom", None, None));
    assert!(caps.url_supported("https", Some("csv"), None));
    assert!(!caps.url_supported("spark", None, None));
}

#[tokio::test]
async fn test_planner_capabilities_from_runtime() {
    let schema = get_movies_schema();
    let scanner = CustomSchemeScanner { schema };

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(scanner)]);
    let caps = runtime.planner_capabilities();
    assert!(caps.supported_schemes.contains("custom"));
    assert!(caps.supported_schemes.contains("http"));
}

/// Test a resolver that returns ResolutionResult::Table directly (bypassing DataFusion execution).
#[tokio::test]
async fn test_table_returning_resolver() {
    // A resolver that materializes the plan itself, returning a Table directly.
    struct TableResolver {
        movies_table: Arc<dyn TableProvider>,
    }

    #[async_trait]
    impl PlanResolver for TableResolver {
        fn name(&self) -> &str {
            "TableResolver"
        }

        async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
            // Rewrite ExternalTableProvider -> MemTable, then execute locally
            let mut rewriter = TableRewriter {
                movies_table: self.movies_table.clone(),
            };
            let rewritten = plan.rewrite(&mut rewriter).unwrap().data;

            // Execute via a local SessionContext
            let ctx = datafusion::prelude::SessionContext::new();
            let df = datafusion::prelude::DataFrame::new(ctx.state(), rewritten);
            let batches = df.collect().await.unwrap();
            let table = VegaFusionTable::try_new(batches[0].schema(), batches).unwrap();

            Ok(ResolutionResult::Table(table))
        }
    }

    let movies_table = create_movies_table();
    let schema = movies_table.schema.clone();
    let batches = movies_table.batches.clone();
    let mem_table =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()) as Arc<dyn TableProvider>;

    let resolver = TableResolver {
        movies_table: mem_table,
    };
    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(resolver)]);

    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());
}

/// Test that VegaFusionRuntime works with no resolver (None) when inline datasets are tables.
#[tokio::test]
async fn test_no_resolver() {
    let runtime = VegaFusionRuntime::new(None, Vec::new());

    let spec_str = r#"{
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "width": 400,
        "height": 200,
        "padding": 5,
        "data": [
            {
                "name": "source_0",
                "url": "table://movies",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["genre"],
                        "ops": ["count"],
                        "fields": [null],
                        "as": ["movie_count"]
                    }
                ]
            }
        ],
        "marks": [
            {
                "type": "rect",
                "from": {"data": "source_0"},
                "encode": {
                    "enter": {
                        "x": {"field": "genre"},
                        "y": {"field": "movie_count"}
                    }
                }
            }
        ]
    }"#;

    let spec: ChartSpec = serde_json::from_str(spec_str).unwrap();

    // Provide inline datasets as tables (not plans), so no resolver is needed
    let movies_table = create_movies_table();
    let mut inline_datasets = std::collections::HashMap::new();
    inline_datasets.insert(
        "movies".to_string(),
        VegaFusionDataset::from_table(movies_table, None).unwrap(),
    );

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::col;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
    };
    use datafusion_proto::protobuf::LogicalPlanNode;
    use prost::Message;
    use vegafusion_runtime::data::codec::VegaFusionCodec;

    /// Round-trip test: serialize a LogicalPlan with ExternalTableProvider,
    /// deserialize back, and verify structural equivalence.
    #[tokio::test]
    async fn test_external_table_proto_round_trip() {
        let schema = get_movies_schema();
        let provider = Arc::new(ExternalTableProvider::new(
            schema,
            Some("test".to_string()),
            serde_json::Value::Null,
        ));
        let table_source = provider_as_source(provider);

        // Build: TableScan -> Filter -> Projection
        let plan = LogicalPlanBuilder::scan("movies", table_source, None)
            .unwrap()
            .filter(col("release_year").gt(datafusion_expr::lit(2000)))
            .unwrap()
            .project(vec![col("title"), col("genre")])
            .unwrap()
            .build()
            .unwrap();

        let codec = VegaFusionCodec::new();

        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap();
        let ctx = SessionContext::new();
        let round_tripped =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec).unwrap();

        assert_eq!(format!("{plan:?}"), format!("{round_tripped:?}"));
    }

    /// Raw proto inspection: decode as LogicalPlanNode without a codec,
    /// walk the tree, and verify the CustomScan node has the correct
    /// table name and schema. This simulates what another would see when deserializing
    /// with protoc-generated classes.
    #[tokio::test]
    async fn test_external_table_raw_proto_inspection() {
        let schema = get_movies_schema();
        let provider = Arc::new(ExternalTableProvider::new(
            schema.clone(),
            Some("test".to_string()),
            serde_json::Value::Null,
        ));
        let table_source = provider_as_source(provider);

        // Build: TableScan -> Filter(release_year > 2000)
        let plan = LogicalPlanBuilder::scan("movies", table_source, None)
            .unwrap()
            .filter(col("release_year").gt(datafusion_expr::lit(2000)))
            .unwrap()
            .build()
            .unwrap();

        let codec = VegaFusionCodec::new();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap();

        // Decode as raw proto without a custom codec
        let proto = LogicalPlanNode::decode(bytes.as_ref()).unwrap();

        // The top-level node should be a Selection (Filter)
        use datafusion_proto::protobuf::logical_plan_node::LogicalPlanType;
        let plan_type = proto.logical_plan_type.as_ref().unwrap();
        let selection = match plan_type {
            LogicalPlanType::Selection(sel) => sel,
            other => panic!("Expected Selection (Filter), got {:?}", other),
        };

        // The input to the filter should be the CustomScan
        let input = selection.input.as_ref().unwrap();
        let input_type = input.logical_plan_type.as_ref().unwrap();
        let custom_scan = match input_type {
            LogicalPlanType::CustomScan(scan) => scan,
            other => panic!("Expected CustomScan, got {:?}", other),
        };

        let table_ref = custom_scan.table_name.as_ref().unwrap();
        use datafusion_proto::protobuf::table_reference::TableReferenceEnum;
        match table_ref.table_reference_enum.as_ref().unwrap() {
            TableReferenceEnum::Bare(bare) => assert_eq!(bare.table, "movies"),
            other => panic!("Expected Bare table reference, got {:?}", other),
        }

        let proto_schema = custom_scan.schema.as_ref().unwrap();
        let field_names: Vec<&str> = proto_schema
            .columns
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(
            field_names,
            vec![
                "title",
                "genre",
                "director",
                "release_year",
                "worldwide_gross",
                "production_budget",
                "imdb_rating",
                "rotten_tomatoes",
            ]
        );

        let envelope: serde_json::Value =
            serde_json::from_slice(&custom_scan.custom_table_data).unwrap();
        assert_eq!(envelope["type"], "external");
        assert!(envelope["metadata"].is_null());
    }

    /// Round-trip test with JSON metadata: verify metadata survives
    /// serialization and is visible in raw proto as custom_table_data.
    #[tokio::test]
    async fn test_external_table_metadata_round_trip() {
        let schema = get_movies_schema();
        let metadata = serde_json::json!({
            "source": "postgres",
            "table": "public.movies",
            "filters": [{"col": "year", "op": ">", "val": 2000}],
        });
        let provider = Arc::new(ExternalTableProvider::new(
            schema.clone(),
            Some("test".to_string()),
            metadata.clone(),
        ));
        let table_source = provider_as_source(provider);

        let plan = LogicalPlanBuilder::scan("movies", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        let codec = VegaFusionCodec::new();

        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap();
        let ctx = SessionContext::new();
        let round_tripped =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec).unwrap();

        if let DFLogicalPlan::TableScan(scan) = &round_tripped {
            let provider = datafusion::datasource::source_as_provider(&scan.source).unwrap();
            let ext = provider
                .as_any()
                .downcast_ref::<ExternalTableProvider>()
                .expect("Expected ExternalTableProvider");
            assert_eq!(ext.protocol(), Some("test"));
            assert_eq!(ext.metadata(), &metadata);
        } else {
            panic!("Expected TableScan, got {:?}", round_tripped);
        }

        // Also verify raw proto has the JSON in custom_table_data
        let proto = LogicalPlanNode::decode(bytes.as_ref()).unwrap();
        use datafusion_proto::protobuf::logical_plan_node::LogicalPlanType;
        let custom_scan = match proto.logical_plan_type.as_ref().unwrap() {
            LogicalPlanType::CustomScan(scan) => scan,
            other => panic!("Expected CustomScan, got {:?}", other),
        };
        let decoded: serde_json::Value =
            serde_json::from_slice(&custom_scan.custom_table_data).unwrap();
        assert_eq!(decoded["type"], "external");
        assert_eq!(decoded["metadata"], metadata);
    }

    /// Round-trip test for InlineTableProvider: encode a plan with InlineTableProvider,
    /// then decode with a sidecar containing the table data, and verify the decoded
    /// plan has a MemTable with the correct data.
    #[tokio::test]
    async fn test_inline_table_codec_round_trip() {
        use vegafusion_runtime::data::inline_table::InlineTableProvider;

        let schema = get_movies_schema();
        let movies = create_movies_table();

        let provider = Arc::new(InlineTableProvider::new(
            schema.clone(),
            "movies".to_string(),
        ));
        let table_source = provider_as_source(provider);

        let plan = LogicalPlanBuilder::scan("movies_inline", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        // Encode with VegaFusionCodec (no sidecar needed for encoding)
        let codec = VegaFusionCodec::new();
        let bytes = logical_plan_to_bytes_with_extension_codec(&plan, &codec).unwrap();

        // Decode with sidecar containing the actual table data
        let mut sidecar = std::collections::HashMap::new();
        sidecar.insert("movies".to_string(), movies.batches.clone());

        let decode_codec = VegaFusionCodec::with_sidecar(sidecar);
        let ctx = SessionContext::new();
        let decoded =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &decode_codec)
                .unwrap();

        // The decoded plan should have a MemTable (not InlineTableProvider)
        if let DFLogicalPlan::TableScan(scan) = &decoded {
            let provider = datafusion::datasource::source_as_provider(&scan.source).unwrap();
            // Should be a MemTable, not InlineTableProvider
            assert!(
                provider.as_any().downcast_ref::<MemTable>().is_some(),
                "Expected MemTable after decoding with sidecar"
            );
            assert_eq!(provider.schema().fields().len(), schema.fields().len());
        } else {
            panic!("Expected TableScan, got {:?}", decoded);
        }

        let df = datafusion::prelude::DataFrame::new(ctx.state(), decoded);
        let result_batches = df.collect().await.unwrap();
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10, "Expected 10 rows from the movies table");
    }
}

/// Test that resolver errors propagate correctly through the runtime.
#[tokio::test]
async fn test_resolver_error_propagation() {
    struct FailingResolver;

    #[async_trait]
    impl PlanResolver for FailingResolver {
        fn name(&self) -> &str {
            "FailingResolver"
        }

        async fn resolve_plan(&self, _plan: LogicalPlan) -> Result<ResolutionResult> {
            Err(vegafusion_common::error::VegaFusionError::internal(
                "resolver deliberately failed",
            ))
        }
    }

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(FailingResolver)]);

    let spec = get_simple_spec();
    let inline_datasets = get_inline_datasets();

    let result = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,
            &PreTransformSpecOpts {
                preserve_interactivity: false,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
                row_limit: None,
                keep_variables: vec![],
                data_base_url: None,
            },
        )
        .await;

    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("resolver deliberately failed"),
        "Error should contain resolver message, got: {msg}"
    );
}

#[tokio::test]
async fn test_execute_plan_pipeline_propagates_resolver_errors() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let failing_resolver = ScriptedResolver::new(
        "failing",
        ResolverBehavior::Fail("resolver deliberately failed"),
        events,
    );

    let resolvers: Vec<Arc<dyn PlanResolver>> = vec![Arc::new(failing_resolver)];
    let ctx = datafusion::prelude::SessionContext::new();
    let plan = build_external_scan_plan("movies");

    let pipeline = ResolverPipeline::new(resolvers, Arc::new(ctx));
    let err = pipeline.resolve(plan).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("resolver deliberately failed"),
        "Error should contain resolver message, got: {msg}"
    );
}

#[tokio::test]
async fn test_datafusion_resolver_executes_simple_plan() {
    use vegafusion_runtime::data::datafusion_resolver::DataFusionResolver;

    let ctx = Arc::new(datafusion::prelude::SessionContext::new());
    let table = create_movies_table();
    let mem_table = MemTable::try_new(table.schema.clone(), vec![table.batches]).unwrap();
    ctx.register_table("movies_df_resolver", Arc::new(mem_table))
        .unwrap();

    let plan = ctx
        .table("movies_df_resolver")
        .await
        .unwrap()
        .into_unoptimized_plan();

    let resolver = DataFusionResolver::new(ctx);
    let result = resolver.resolve_plan(plan).await.unwrap();
    match result {
        ResolutionResult::Table(t) => assert_eq!(table_row_count(&t), 10),
        ResolutionResult::Plan(_) => panic!("DataFusionResolver should always return Table"),
    }
}

#[tokio::test]
async fn test_resolver_pipeline_has_user_resolvers() {
    let ctx = Arc::new(datafusion::prelude::SessionContext::new());

    let empty_pipeline = ResolverPipeline::new(vec![], ctx.clone());
    assert!(
        !empty_pipeline.has_user_resolvers(),
        "Empty pipeline should report no user resolvers"
    );

    let events = Arc::new(Mutex::new(Vec::new()));
    let resolver = ScriptedResolver::new("test", ResolverBehavior::PassThroughPlan, events);
    let resolvers: Vec<Arc<dyn PlanResolver>> = vec![Arc::new(resolver)];
    let pipeline_with_resolvers = ResolverPipeline::new(resolvers, ctx);
    assert!(
        pipeline_with_resolvers.has_user_resolvers(),
        "Pipeline with resolvers should report has user resolvers"
    );
}
