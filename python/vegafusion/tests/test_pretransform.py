import pandas as pd
import vegafusion as vf
import json


def order_items_spec():
    return r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "height": 200,
  "style": "cell",
  "data": [
    {"name": "order_items", "url": "vegafusion+dataset://order_items"},
    {
      "name": "data_0",
      "source": "order_items",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["menu_item"],
          "ops": ["count"],
          "fields": ["menu_item"],
          "as": ["__count"]
        }
      ]
    }
  ],
  "signals": [
    {"name": "x_step", "value": 20},
    {
      "name": "width",
      "update": "bandspace(domain('x').length, 0.1, 0.05) * x_step"
    }
  ],
  "marks": [
    {
      "name": "layer_0_marks",
      "type": "rect",
      "clip": true,
      "style": ["bar"],
      "from": {"data": "data_0"},
      "encode": {
        "update": {
          "fill": {"value": "#4c78a8"},
          "x": {"scale": "x", "field": "menu_item"},
          "width": {"scale": "x", "band": 1},
          "y": {"scale": "y", "field": "__count"},
          "y2": {"scale": "y", "value": 0}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "band",
      "domain": {"data": "data_0", "field": "menu_item", "sort": true},
      "range": {"step": {"signal": "x_step"}},
      "paddingInner": 0.1,
      "paddingOuter": 0.05
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "data_0", "field": "__count"},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    }
  ]
}
"""


def movies_histogram_spec(agg = "count"):
    return """
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "width": 200,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "source_0",
      "url": "data/movies.json",
      "format": {"type": "json"},
      "transform": [
        {
          "type": "extent",
          "field": "IMDB Rating",
          "signal": "bin_maxbins_10_IMDB_Rating_extent"
        },
        {
          "type": "bin",
          "field": "IMDB Rating",
          "as": [
            "bin_maxbins_10_IMDB Rating",
            "bin_maxbins_10_IMDB Rating_end"
          ],
          "signal": "bin_maxbins_10_IMDB_Rating_bins",
          "extent": {"signal": "bin_maxbins_10_IMDB_Rating_extent"},
          "maxbins": 10
        },
        {
          "type": "aggregate",
          "groupby": [
            "bin_maxbins_10_IMDB Rating",
            "bin_maxbins_10_IMDB Rating_end"
          ],
          "ops": [""" + '"' + agg + '"' + r"""],
          "fields": ["Worldwide Gross"],
          "as": ["median_Worldwide Gross"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_10_IMDB Rating\"]) && isFinite(+datum[\"bin_maxbins_10_IMDB Rating\"]) && isValid(datum[\"median_Worldwide Gross\"]) && isFinite(+datum[\"median_Worldwide Gross\"])"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "marks",
      "type": "rect",
      "style": ["bar"],
      "from": {"data": "source_0"},
      "encode": {
        "update": {
          "fill": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"IMDB Rating (binned): \" + (!isValid(datum[\"bin_maxbins_10_IMDB Rating\"]) || !isFinite(+datum[\"bin_maxbins_10_IMDB Rating\"]) ? \"null\" : format(datum[\"bin_maxbins_10_IMDB Rating\"], \"\") + \" – \" + format(datum[\"bin_maxbins_10_IMDB Rating_end\"], \"\")) + \"; Median of Worldwide Gross: \" + (format(datum[\"median_Worldwide Gross\"], \"\"))"
          },
          "x2": {
            "scale": "x",
            "field": "bin_maxbins_10_IMDB Rating",
            "offset": 1
          },
          "x": {"scale": "x", "field": "bin_maxbins_10_IMDB Rating_end"},
          "y": {"scale": "y", "field": "median_Worldwide Gross"},
          "y2": {"scale": "y", "value": 0}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "linear",
      "domain": {
        "signal": "[bin_maxbins_10_IMDB_Rating_bins.start, bin_maxbins_10_IMDB_Rating_bins.stop]"
      },
      "range": [0, {"signal": "width"}],
      "bins": {"signal": "bin_maxbins_10_IMDB_Rating_bins"},
      "zero": false
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "source_0", "field": "median_Worldwide Gross"},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    }
  ],
  "axes": [
    {
      "scale": "y",
      "orient": "left",
      "gridScale": "x",
      "grid": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "domain": false,
      "labels": false,
      "aria": false,
      "maxExtent": 0,
      "minExtent": 0,
      "ticks": false,
      "zindex": 0
    },
    {
      "scale": "x",
      "orient": "bottom",
      "grid": false,
      "title": "IMDB Rating (binned)",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/10)"},
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "Median of Worldwide Gross",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ]
}"""


def test_pre_transform_multi_partition():
    n = 4050
    order_items = pd.DataFrame({
        "menu_item": [0] * n + [1] * n
    })

    vega_spec = order_items_spec()
    new_spec, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC", inline_datasets={
        "order_items": order_items,
    })
    new_spec = json.loads(new_spec)
    assert new_spec["data"][1] == dict(
        name="data_0",
        values=[
            {"menu_item": 0, "__count": n},
            {"menu_item": 1, "__count": n},
        ]
    )


def test_pre_transform_cache_cleared():
    # Make sure that result changes when input DataFrame changes
    def check(n):
        order_items = pd.DataFrame({
            "menu_item": [0] * n + [1] * n
        })

        vega_spec = order_items_spec()
        new_spec, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC", inline_datasets={
            "order_items": order_items,
        })
        new_spec = json.loads(new_spec)
        assert new_spec["data"][1] == dict(
            name="data_0",
            values=[
                {"menu_item": 0, "__count": n},
                {"menu_item": 1, "__count": n},
            ]
        )

    check(16)
    check(32)


def test_pre_transform_datasets():
    n = 4050
    order_items = pd.DataFrame({
        "menu_item": [0] * n + [1] * (2 * n) + [2] * (3 * n)
    })

    vega_spec = order_items_spec()
    datasets, warnings = vf.runtime.pre_transform_datasets(
        vega_spec,
        ["data_0"],
        "UTC",
        inline_datasets={
            "order_items": order_items,
        }
    )
    assert len(warnings) == 0
    assert len(datasets) == 1

    expected = pd.DataFrame({"menu_item": [0, 1, 2], "__count": [n, 2 * n, 3 * n]})
    pd.testing.assert_frame_equal(datasets[0], expected)


def test_pre_transform_planner_warning():
    # Pre-transform with supported aggregate function should result in no warnings
    vega_spec = movies_histogram_spec("mean")
    datasets, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC")
    assert len(warnings) == 0

    # Pre-transform with unsupported aggregate function should result in one warning
    vega_spec = movies_histogram_spec("ci0")
    datasets, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC")
    assert len(warnings) == 1

    warning = warnings[0]
    assert warning["type"] == "Planner"
    assert "source_0" in warning["message"]
