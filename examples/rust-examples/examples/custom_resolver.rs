use std::sync::Arc;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
use vegafusion_core::runtime::{PlanResolver, ResolutionResult, VegaFusionRuntimeTrait};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// A custom resolver that logs plan resolution and passes through to DataFusion
#[derive(Clone)]
struct LoggingResolver;

#[async_trait::async_trait]
impl PlanResolver for LoggingResolver {
    fn name(&self) -> &str {
        "LoggingResolver"
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        println!("Custom resolver received logical plan");
        println!("Plan details:\n{}\n", plan.display_indent());

        // Return the plan unchanged — DataFusion will execute it
        Ok(ResolutionResult::Plan(plan))
    }
}

/// This example demonstrates how to use a custom plan resolver with VegaFusion.
/// The custom resolver logs each plan before letting DataFusion execute it.
#[tokio::main]
async fn main() {
    let spec = get_spec();

    // Create a custom resolver
    let custom_resolver = Arc::new(LoggingResolver) as Arc<dyn PlanResolver>;

    // Create runtime with custom resolver
    let runtime = VegaFusionRuntime::new(None, vec![custom_resolver]);

    println!("Starting pre-transform with custom resolver\n");

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
      "description": "A histogram demonstrating custom resolver usage",
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
