use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlanBuilder;
use datafusion_expr::{Expr, LogicalPlan as DFLogicalPlan, TableSource};
use std::any::Any;
use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use vegafusion_common::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
use vegafusion_core::runtime::{PlanExecutor, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use vegafusion_runtime::plan_executor::DataFusionPlanExecutor;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

// By-default, in DataFusion you construct table provider (i.e. enity which can actually load data and return
// to execution engine) and then create table source (schema-only entity used for logical plan) from it. To make sure
// we don't accidentally execute logical plan bypassing plan executor, for tests we implement custom table source which
// can't load any data. Trying to execute logical plan with this table source will result in an error.
#[derive(Debug, Clone)]
struct SchemaOnlyTableSource {
    schema: Arc<Schema>,
}

impl SchemaOnlyTableSource {
    fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl TableSource for SchemaOnlyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<datafusion_expr::TableProviderFilterPushDown>> {
        Ok(vec![])
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, DFLogicalPlan>> {
        None
    }
}

// Custom executor which tracks its invocations and passes actual query execution to DataFusion executor, rewriting
// plan to replace our custom table source with mem table to make query executable.
#[derive(Clone)]
struct TrackingPlanExecutor {
    call_count: Arc<AtomicUsize>,
    movies_table: Arc<dyn TableProvider>,
    fallback_executor: Arc<DataFusionPlanExecutor>,
}

impl TrackingPlanExecutor {
    fn new() -> Self {
        let ctx = Arc::new(make_datafusion_context());

        let movies_table = create_movies_table();
        let schema = movies_table.schema.clone();
        let batches = movies_table.batches.clone();
        let mem_table =
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()) as Arc<dyn TableProvider>;

        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
            movies_table: mem_table,
            fallback_executor: Arc::new(DataFusionPlanExecutor::new(ctx)),
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
            if scan.table_name.table() == "movies" {
                // Verify that the source is actually our SchemaOnlyTableSource
                if scan
                    .source
                    .as_any()
                    .downcast_ref::<SchemaOnlyTableSource>()
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
        }
        Ok(Transformed::no(node))
    }
}

#[async_trait]
impl PlanExecutor for TrackingPlanExecutor {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        let mut rewriter = TableRewriter {
            movies_table: self.movies_table.clone(),
        };
        let rewritten_plan = plan.rewrite(&mut rewriter).unwrap().data;

        self.fallback_executor.execute_plan(rewritten_plan).await
    }
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_spec() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();

    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));

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
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = executor_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom executor should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_extract() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();

    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));

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
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = executor_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom executor should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_values() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();

    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));

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
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(values.len(), 1);

    let call_count = executor_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom executor should have been called at least once"
    );
}

// Bin transform internally uses data from extent transform to compute binning boundaries.
// This requires materializing extend data before binning can be done, which creates kind of chain
// where binning logical plan will depend on materialized results of extent.
#[tokio::test]
async fn test_bin_transform_uses_custom_executor() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();

    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));

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
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = executor_clone.get_call_count();

    assert_eq!(
        call_count, 3,
        "Custom executor should have been called 3 times: \
        (1) extent transform for binning boundaries, \
        (2) bin + aggregate transforms, \
        (3) scale domain computation for yscale"
    );
}

#[tokio::test]
async fn test_mixed_data_only_executes_plans() {
    let tracking_executor = TrackingPlanExecutor::new();
    let executor_clone = tracking_executor.clone();

    let runtime = VegaFusionRuntime::new(None, Some(Arc::new(tracking_executor)));

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
    let movies_plan = create_movies_logical_plan();

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
            },
        )
        .await
        .unwrap();

    assert!(warnings.is_empty());

    let call_count = executor_clone.get_call_count();

    assert_eq!(
        call_count, 1,
        "Custom executor should have been called only once"
    );
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
    let schema = get_movies_schema();

    let table_source = Arc::new(SchemaOnlyTableSource::new(schema));

    LogicalPlanBuilder::scan("movies", table_source, None)
        .unwrap()
        .build()
        .unwrap()
}

fn get_inline_datasets() -> std::collections::HashMap<String, VegaFusionDataset> {
    let logical_plan = create_movies_logical_plan();
    let dataset = VegaFusionDataset::from_plan(logical_plan);

    let mut datasets = std::collections::HashMap::new();
    datasets.insert("movies".to_string(), dataset);
    datasets
}
