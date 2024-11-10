use std::collections::HashMap;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// This example demonstrates how to use the `pre_transform_spec` method with an inline
/// Arrow table to create a new spec with supported transforms pre-evaluated.
#[tokio::main]
async fn main() {
    let spec = get_spec();

    // Fetch  movies dataset as json
    let client = reqwest::ClientBuilder::new().build().unwrap();
    let movies_json: serde_json::Value = client
        .get(
            "https://raw.githubusercontent.com/vega/vega-datasets/refs/heads/main/data/movies.json",
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // Construct VegaFusionTable and wrap in VegaFusionDataset
    let table = VegaFusionTable::from_json(&movies_json).unwrap();
    let dataset = VegaFusionDataset::from_table(table, None).unwrap();
    let inline_datasets: HashMap<String, VegaFusionDataset> =
        vec![("movies".to_string(), dataset)].into_iter().collect();

    let runtime = VegaFusionRuntime::new(None);

    let (transformed_spec, warnings) = runtime
        .pre_transform_spec(
            &spec,
            &inline_datasets,    // Inline datasets
            &Default::default(), // Options
        )
        .await
        .unwrap();

    assert_eq!(warnings.len(), 0);
    println!(
        "{}",
        serde_json::to_string_pretty(&transformed_spec).unwrap()
    );
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
      "data": [
        {
          "name": "table",
          "url": "vegafusion+dataset://movies",
          "transform": [
            {
              "type": "extent", "field": "IMDB Rating",
              "signal": "extent"
            },
            {
              "type": "bin", "signal": "bins",
              "field": "IMDB Rating", "extent": {"signal": "extent"},
              "maxbins": 10
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
      "signals": [
        {
          "name": "maxbins", "value": 10
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
