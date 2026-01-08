use datafusion::prelude::SessionContext;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::runtime::{PlanExecutor, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use vegafusion_runtime::plan_executor::DataFusionPlanExecutor;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// A custom executor that logs plan execution and forwards to DataFusion
#[derive(Clone)]
struct LoggingExecutor {
    fallback: Arc<DataFusionPlanExecutor>,
}

impl LoggingExecutor {
    fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            fallback: Arc::new(DataFusionPlanExecutor::new(ctx)),
        }
    }
}

#[async_trait::async_trait]
impl PlanExecutor for LoggingExecutor {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<VegaFusionTable> {
        println!("Custom executor received logical plan");
        println!("Plan details:\n{}\n", plan.display_indent());

        // Forward to DataFusion for actual execution
        let result = self.fallback.execute_plan(plan).await?;

        println!(
            "Custom executor executed plan, returned {} rows\n",
            result.num_rows()
        );

        Ok(result)
    }
}

/// This example demonstrates how to use a custom plan executor with VegaFusion.
/// The custom executor logs each plan execution before forwarding to DataFusion.
#[tokio::main]
async fn main() {
    let spec = get_spec();

    // Create a custom executor
    let ctx = Arc::new(make_datafusion_context());
    let custom_executor = Arc::new(LoggingExecutor::new(ctx)) as Arc<dyn PlanExecutor>;

    // Create runtime with custom executor
    let runtime = VegaFusionRuntime::new(None, Some(custom_executor));

    println!("Starting pre-transform with custom executor\n");

    let (_transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &Default::default(), // Inline datasets
            &Default::default(), // Options
        )
        .await
        .unwrap();
    println!("Spec transformed");
    assert_eq!(warnings.len(), 0);
}

fn get_spec() -> ChartSpec {
    let spec_str = r##"
    {
      "$schema": "https://vega.github.io/schema/vega/v5.json",
      "description": "A histogram demonstrating custom executor usage",
      "width": 400,
      "height": 200,
      "padding": 5,
      "data": [
        {
          "name": "table",
          "url": "data/movies.json",
          "transform": [
            {
              "type": "extent", 
              "field": "IMDB Rating",
              "signal": "extent"
            },
            {
              "type": "bin", 
              "signal": "bins",
              "field": "IMDB Rating", 
              "extent": {"signal": "extent"},
              "maxbins": 10
            },
            {
              "type": "aggregate",
              "groupby": ["bin0", "bin1"],
              "ops": ["count"],
              "fields": [null],
              "as": ["count"]
            }
          ]
        }
      ],
      "scales": [
        {
          "name": "xscale",
          "type": "linear",
          "range": "width",
          "domain": {"signal": "extent"}
        },
        {
          "name": "yscale",
          "type": "linear",
          "range": "height",
          "round": true,
          "domain": {"data": "table", "field": "count"},
          "zero": true,
          "nice": true
        }
      ],
      "marks": [
        {
          "type": "rect",
          "from": {"data": "table"},
          "encode": {
            "update": {
              "x": {"scale": "xscale", "field": "bin0"},
              "x2": {"scale": "xscale", "field": "bin1"},
              "y": {"scale": "yscale", "field": "count"},
              "y2": {"scale": "yscale", "value": 0}
            }
          }
        }
      ]
    }
    "##;
    serde_json::from_str(spec_str).unwrap()
}
