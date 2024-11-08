from typing import Any
import json
import vegafusion as vf


# This example demonstrates how to use a chart state, and update it in response to simulated interactive
# updates to the chart
def main():
    spec = get_spec()

    # Build chart state
    chart_state = vf.runtime.new_chart_state(spec)

    # Get the initial pre-transformed spec that can be rendered
    _init_spec = chart_state.get_client_spec()

    # Get the watch plan, which includes which signals and data variables that should be listened to
    # and relayed from the displayed vega chart back to the chart state.
    watch_plan = chart_state.get_watch_plan()
    print("Watch Plan:\n" + json.dumps(watch_plan, indent=2), end="\n\n")

    # Report an update to the maxbins signal. Update will return the signal and dataset updates that should
    # but updated in the displayed chart.
    updates = chart_state.update(
        [
            {
                "name": "maxbins",
                "namespace": "signal",
                "scope": [],
                "value": 4,
            }
        ]
    )

    print("Server to Client Updates:\n" + json.dumps(updates, indent=2), end="\n\n")


def get_spec() -> dict[str, Any]:
    """
    Based on https://vega.github.io/editor/#/examples/vega/histogram-null-values
    """
    spec_str = """
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
    """
    return json.loads(spec_str)


def expected_spec() -> dict[str, Any]:
    return json.loads("""
    {
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "table"
    },
    {
      "name": "counts",
      "values": [
        {
          "bin0": 6.0,
          "bin1": 7.0,
          "count": 985
        },
        {
          "bin0": 3.0,
          "bin1": 4.0,
          "count": 100
        },
        {
          "bin0": 7.0,
          "bin1": 8.0,
          "count": 741
        },
        {
          "bin0": 5.0,
          "bin1": 6.0,
          "count": 633
        },
        {
          "bin0": 8.0,
          "bin1": 9.0,
          "count": 204
        },
        {
          "bin0": 2.0,
          "bin1": 3.0,
          "count": 43
        },
        {
          "bin0": 4.0,
          "bin1": 5.0,
          "count": 273
        },
        {
          "bin0": 9.0,
          "bin1": 10.0,
          "count": 4
        },
        {
          "bin0": 1.0,
          "bin1": 2.0,
          "count": 5
        }
      ]
    },
    {
      "name": "nulls",
      "values": [
        {
          "count": 213
        }
      ]
    }
  ],
  "signals": [
    {
      "name": "bins",
      "value": {
        "fields": [
          "IMDB Rating"
        ],
        "fname": "bin_IMDB Rating",
        "start": 1.0,
        "step": 1.0,
        "stop": 10.0
      }
    },
    {
      "name": "maxbins",
      "value": 10
    },
    {
      "name": "binCount",
      "update": "(bins.stop - bins.start) / bins.step"
    },
    {
      "name": "nullGap",
      "value": 10
    },
    {
      "name": "barStep",
      "update": "(width - nullGap) / (1 + binCount)"
    }
  ],
  "marks": [
    {
      "type": "rect",
      "from": {
        "data": "counts"
      },
      "encode": {
        "update": {
          "y": {
            "field": "count",
            "scale": "yscale"
          },
          "fill": {
            "value": "steelblue"
          },
          "x2": {
            "field": "bin1",
            "scale": "xscale"
          },
          "x": {
            "field": "bin0",
            "scale": "xscale",
            "offset": 1
          },
          "y2": {
            "value": 0,
            "scale": "yscale"
          }
        },
        "hover": {
          "fill": {
            "value": "firebrick"
          }
        }
      }
    },
    {
      "type": "rect",
      "from": {
        "data": "nulls"
      },
      "encode": {
        "hover": {
          "fill": {
            "value": "firebrick"
          }
        },
        "update": {
          "x2": {
            "scale": "xscale-null",
            "band": 1
          },
          "y": {
            "field": "count",
            "scale": "yscale"
          },
          "y2": {
            "value": 0,
            "scale": "yscale"
          },
          "fill": {
            "value": "#aaa"
          },
          "x": {
            "scale": "xscale-null",
            "offset": 1
          }
        }
      }
    }
  ],
  "scales": [
    {
      "name": "yscale",
      "type": "linear",
      "domain": {
        "fields": [
          {
            "data": "counts",
            "field": "count"
          },
          {
            "data": "nulls",
            "field": "count"
          }
        ]
      },
      "range": "height",
      "nice": true,
      "round": true
    },
    {
      "name": "xscale",
      "type": "linear",
      "domain": {
        "signal": "[bins.start, bins.stop]"
      },
      "range": [
        {
          "signal": "barStep + nullGap"
        },
        {
          "signal": "width"
        }
      ],
      "bins": {
        "signal": "bins"
      },
      "round": true
    },
    {
      "name": "xscale-null",
      "type": "band",
      "domain": [
        null
      ],
      "range": [
        0,
        {
          "signal": "barStep"
        }
      ],
      "round": true
    }
  ],
  "axes": [
    {
      "scale": "xscale",
      "tickMinStep": 0.5,
      "orient": "bottom"
    },
    {
      "scale": "xscale-null",
      "orient": "bottom"
    },
    {
      "scale": "yscale",
      "tickCount": 5,
      "offset": 5,
      "orient": "left"
    }
  ],
  "width": 400,
  "height": 200,
  "description": "A histogram of film ratings, modified to include null values.",
  "padding": 5,
  "autosize": {
    "type": "fit",
    "resize": true
  }
}
    """)


if __name__ == "__main__":
    main()
