use vegafusion_core::chart_state::ChartState;
use vegafusion_core::planning::watch::{ExportUpdateJSON, ExportUpdateNamespace};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// This example demonstrates how to use the `pre_transform_spec` method to create a new
/// spec with supported transforms pre-evaluated.
#[tokio::main]
async fn main() {
    let spec = get_spec();

    // Make runtime
    let runtime = VegaFusionRuntime::new(None);

    // Construct ChartState
    let chart_state = ChartState::try_new(
        &runtime,
        spec,
        Default::default(),  // Inline datasets
        &Default::default(), // Options
    )
    .await
    .unwrap();

    // Get initial transformed spec for display
    let _transformed_spec = chart_state.get_client_spec();

    // Get comm plan
    let comm_plan = chart_state.get_comm_plan();
    println!("{:#?}", comm_plan);

    // Apply an update to the maxbins signal
    let updates = chart_state
        .update(
            &runtime,
            vec![ExportUpdateJSON {
                namespace: ExportUpdateNamespace::Signal,
                name: "maxbins".to_string(),
                scope: vec![],
                value: 4.into(),
            }],
        )
        .await
        .unwrap();

    // Print updates that should be applied to the rendered Vega chart
    println!("{}", serde_json::to_string_pretty(&updates).unwrap());
}

fn get_spec() -> ChartSpec {
    let spec_str = r##"
    {
      "$schema": "https://vega.github.io/schema/vega/v5.json",
      "description": "A histogram of film ratings, modified to include null values.",
      "width": 400,
      "height": 200,
      "padding": 5,
      "autosize": {"type": "fit", "resize": true},

      "signals": [
        {
          "name": "maxbins", "value": 10,
          "bind": {"input": "select", "options": [5, 10, 20]}
        },
        {
          "name": "binCount",
          "update": "(bins.stop - bins.start) / bins.step"
        },
        {
          "name": "nullGap", "value": 10
        },
        {
          "name": "barStep",
          "update": "(width - nullGap) / (1 + binCount)"
        }
      ],
      "data": [
        {
          "name": "table",
          "url": "data/movies.json",
          "transform": [
            {
              "type": "extent", "field": "IMDB Rating",
              "signal": "extent"
            },
            {
              "type": "bin", "signal": "bins",
              "field": "IMDB Rating", "extent": {"signal": "extent"},
              "maxbins": {"signal": "maxbins"}
            }
          ]
        },
        {
          "name": "counts",
          "source": "table",
          "transform": [
            {
              "type": "filter",
              "expr": "datum['IMDB Rating'] != null"
            },
            {
              "type": "aggregate",
              "groupby": ["bin0", "bin1"]
            }
          ]
        },
        {
          "name": "nulls",
          "source": "table",
          "transform": [
            {
              "type": "filter",
              "expr": "datum['IMDB Rating'] == null"
            },
            {
              "type": "aggregate",
              "groupby": []
            }
          ]
        }
      ],

      "scales": [
        {
          "name": "yscale",
          "type": "linear",
          "range": "height",
          "round": true, "nice": true,
          "domain": {
            "fields": [
              {"data": "counts", "field": "count"},
              {"data": "nulls", "field": "count"}
            ]
          }
        },
        {
          "name": "xscale",
          "type": "linear",
          "range": [{"signal": "barStep + nullGap"}, {"signal": "width"}],
          "round": true,
          "domain": {"signal": "[bins.start, bins.stop]"},
          "bins": {"signal": "bins"}
        },
        {
          "name": "xscale-null",
          "type": "band",
          "range": [0, {"signal": "barStep"}],
          "round": true,
          "domain": [null]
        }
      ],

      "axes": [
        {"orient": "bottom", "scale": "xscale", "tickMinStep": 0.5},
        {"orient": "bottom", "scale": "xscale-null"},
        {"orient": "left", "scale": "yscale", "tickCount": 5, "offset": 5}
      ],

      "marks": [
        {
          "type": "rect",
          "from": {"data": "counts"},
          "encode": {
            "update": {
              "x": {"scale": "xscale", "field": "bin0", "offset": 1},
              "x2": {"scale": "xscale", "field": "bin1"},
              "y": {"scale": "yscale", "field": "count"},
              "y2": {"scale": "yscale", "value": 0},
              "fill": {"value": "steelblue"}
            },
            "hover": {
              "fill": {"value": "firebrick"}
            }
          }
        },
        {
          "type": "rect",
          "from": {"data": "nulls"},
          "encode": {
            "update": {
              "x": {"scale": "xscale-null", "value": null, "offset": 1},
              "x2": {"scale": "xscale-null", "band": 1},
              "y": {"scale": "yscale", "field": "count"},
              "y2": {"scale": "yscale", "value": 0},
              "fill": {"value": "#aaa"}
            },
            "hover": {
              "fill": {"value": "firebrick"}
            }
          }
        }
      ]
    }
    "##;
    serde_json::from_str(spec_str).unwrap()
}
