use vegafusion_core::{get_column_usage, spec::chart::ChartSpec};
use vegafusion_core::proto::gen::tasks::Variable;
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

/// This example demonstrates how to use the `pre_transform_values` method to get
/// transformed datasets as Arrow tables.
#[tokio::main]
async fn main() {
    let spec = get_spec();

    let runtime = VegaFusionRuntime::new(None);

    let (values, warnings) = runtime.pre_transform_values(
        &spec,
        &[(Variable::new_data("counts"), vec![])],
        &Default::default(),  // Inline datasets
        &Default::default()   // Options
    ).await.unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(warnings.len(), 0);

    let TaskValue::Table(counts_table) = &values[0] else {
        panic!("Expected a table")
    };

    let tbl_repr = counts_table.pretty_format(None).unwrap();

    assert_eq!(tbl_repr, "\
+------+------+-------+
| bin0 | bin1 | count |
+------+------+-------+
| 6.0  | 7.0  | 985   |
| 3.0  | 4.0  | 100   |
| 7.0  | 8.0  | 741   |
| 5.0  | 6.0  | 633   |
| 8.0  | 9.0  | 204   |
| 2.0  | 3.0  | 43    |
| 4.0  | 5.0  | 273   |
| 9.0  | 10.0 | 4     |
| 1.0  | 2.0  | 5     |
+------+------+-------+")
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
          "url": "data/movies.json",
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
