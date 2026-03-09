use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::LogicalPlan as DFLogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use vegafusion_common::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::proto::gen::pretransform::PreTransformSpecOpts;
use vegafusion_core::runtime::{PlanResolver, ResolutionResult, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::data::external_table::ExternalTableProvider;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

// Custom resolver which tracks its invocations and rewrites plans to replace
// ExternalTableProvider with MemTable, returning the rewritten plan for DataFusion to execute.
#[derive(Clone)]
struct TrackingResolver {
    call_count: Arc<AtomicUsize>,
    movies_table: Arc<dyn TableProvider>,
}

impl TrackingResolver {
    fn new() -> Self {
        let movies_table = create_movies_table();
        let schema = movies_table.schema.clone();
        let batches = movies_table.batches.clone();
        let mem_table =
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()) as Arc<dyn TableProvider>;

        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
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
            if scan.table_name.table() == "movies" {
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
        }
        Ok(Transformed::no(node))
    }
}

#[async_trait]
impl PlanResolver for TrackingResolver {
    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        let mut rewriter = TableRewriter {
            movies_table: self.movies_table.clone(),
        };
        let rewritten_plan = plan.rewrite(&mut rewriter).unwrap().data;

        Ok(ResolutionResult::Plan(rewritten_plan))
    }
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_spec() {
    let tracking_resolver = TrackingResolver::new();
    let resolver_clone = tracking_resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(tracking_resolver)]);

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

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_extract() {
    let tracking_resolver = TrackingResolver::new();
    let resolver_clone = tracking_resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(tracking_resolver)]);

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

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

#[tokio::test]
async fn test_custom_executor_called_in_pre_transform_values() {
    let tracking_resolver = TrackingResolver::new();
    let resolver_clone = tracking_resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(tracking_resolver)]);

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

    let call_count = resolver_clone.get_call_count();
    assert!(
        call_count > 0,
        "Custom resolver should have been called at least once"
    );
}

// Bin transform internally uses data from extent transform to compute binning boundaries.
// This requires materializing extend data before binning can be done, which creates kind of chain
// where binning logical plan will depend on materialized results of extent.
#[tokio::test]
async fn test_bin_transform_uses_custom_executor() {
    let tracking_resolver = TrackingResolver::new();
    let resolver_clone = tracking_resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(tracking_resolver)]);

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

    let call_count = resolver_clone.get_call_count();

    assert_eq!(
        call_count, 3,
        "Custom resolver should have been called 3 times: \
        (1) extent transform for binning boundaries, \
        (2) bin + aggregate transforms, \
        (3) scale domain computation for yscale"
    );
}

#[tokio::test]
async fn test_mixed_data_only_executes_plans() {
    let tracking_resolver = TrackingResolver::new();
    let resolver_clone = tracking_resolver.clone();

    let runtime = VegaFusionRuntime::new(None, vec![Arc::new(tracking_resolver)]);

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

    let call_count = resolver_clone.get_call_count();

    assert_eq!(
        call_count, 1,
        "Custom resolver should have been called only once"
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

    let provider = Arc::new(ExternalTableProvider::new(
        schema,
        "test".to_string(),
        serde_json::Value::Null,
    ));
    let table_source = provider_as_source(provider);

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

/// Test a resolver that returns ResolutionResult::Table directly (bypassing DataFusion execution).
#[tokio::test]
async fn test_table_returning_resolver() {
    // A resolver that materializes the plan itself, returning a Table directly.
    struct TableResolver {
        movies_table: Arc<dyn TableProvider>,
    }

    #[async_trait]
    impl PlanResolver for TableResolver {
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
            "test".to_string(),
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
            "test".to_string(),
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
            "test".to_string(),
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
            assert_eq!(ext.kind(), "test");
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
