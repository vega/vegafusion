import json
from typing import Any

import vegafusion as vf


def main():
    spec = get_spec()
    res, warnings = vf.runtime.pre_transform_datasets(
        spec, ["counts"], dataset_format="polars"
    )
    assert warnings == []
    assert len(res) == 1
    print(res[0])


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


if __name__ == "__main__":
    main()
