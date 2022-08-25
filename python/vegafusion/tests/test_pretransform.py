import pandas as pd
import vegafusion as vf
import json
from datetime import date

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


def movies_histogram_spec(agg="count"):
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


def standalone_aggregate_spec(agg="count"):
    return """
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "df",
      "values": [
        {
          "x": "a",
          "y": 1
        },
        {
          "x": "a",
          "y": 2
        },
        {
          "x": "a",
          "y": 3
        }
      ]
    },
    {
      "name": "data_0",
      "source": "df",
      "transform": [
        {
          "type": "aggregate",
          "groupby": [
            "x"
          ],
          "fields": [
            "y"
          ],
          "ops": [
            """ + '"' + agg + '"' + """
          ],
          "as": [
            "median_y"
          ]
        },
        {
          "type": "filter",
          "expr": "isValid(datum['median_y']) && isFinite(+datum['median_y'])"
        },
        {
          "type": "project",
          "fields": [
            "median_y",
            "x"
          ]
        }
      ]
    },
    {
      "name": "data_0_x_domain_x",
      "source": "data_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": [
            "x"
          ],
          "fields": [],
          "ops": [],
          "as": []
        },
        {
          "type": "formula",
          "expr": "datum['x']",
          "as": "sort_field"
        }
      ]
    },
    {
      "name": "data_0_y_domain_median_y",
      "source": "data_0",
      "transform": [
        {
          "type": "formula",
          "expr": "+datum['median_y']",
          "as": "median_y"
        },
        {
          "type": "aggregate",
          "groupby": [],
          "fields": [
            "median_y",
            "median_y"
          ],
          "ops": [
            "min",
            "max"
          ],
          "as": [
            "min",
            "max"
          ]
        }
      ]
    }
  ],
  "signals": [
    {
      "name": "width",
      "init": "isFinite(containerSize()[0]) ? containerSize()[0] : 200",
      "on": [
        {
          "events": "window:resize",
          "update": "isFinite(containerSize()[0]) ? containerSize()[0] : 200"
        }
      ]
    },
    {
      "name": "height",
      "init": "isFinite(containerSize()[1]) ? containerSize()[1] : 200",
      "on": [
        {
          "events": "window:resize",
          "update": "isFinite(containerSize()[1]) ? containerSize()[1] : 200"
        }
      ]
    }
  ],
  "marks": [
    {
      "type": "rect",
      "name": "layer_0_marks",
      "from": {
        "data": "data_0"
      },
      "encode": {
        "update": {
          "tooltip": {
            "signal": "{'x': isValid(datum['x']) ? datum['x'] : ''+datum['x'], 'Median of y': format(datum['median_y'], '')}"
          },
          "fill": {
            "value": "#4c78a8"
          },
          "description": {
            "signal": "'x: ' + (isValid(datum['x']) ? datum['x'] : ''+datum['x']) + '; Median of y: ' + (format(datum['median_y'], ''))"
          },
          "width": {
            "scale": "x",
            "band": 1
          },
          "x": {
            "field": "x",
            "scale": "x"
          },
          "y": {
            "field": "median_y",
            "scale": "y"
          },
          "y2": {
            "value": 0,
            "scale": "y"
          },
          "ariaRoleDescription": {
            "value": "bar"
          }
        }
      },
      "style": [
        "bar"
      ],
      "clip": true
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "band",
      "domain": {
        "data": "data_0_x_domain_x",
        "field": "x",
        "sort": true
      },
      "range": [
        0,
        {
          "signal": "width"
        }
      ],
      "paddingOuter": 0.05,
      "paddingInner": 0.1
    },
    {
      "name": "y",
      "type": "linear",
      "domain": [
        {
          "signal": "(data('data_0_y_domain_median_y')[0] || {}).min"
        },
        {
          "signal": "(data('data_0_y_domain_median_y')[0] || {}).max"
        }
      ],
      "range": [
        {
          "signal": "height"
        },
        0
      ],
      "nice": true,
      "zero": true
    }
  ],
  "axes": [
    {
      "scale": "y",
      "grid": true,
      "minExtent": 0,
      "aria": false,
      "orient": "left",
      "gridScale": "x",
      "zindex": 0,
      "tickCount": {
        "signal": "ceil(height/40)"
      },
      "maxExtent": 0,
      "labels": false,
      "ticks": false,
      "domain": false
    },
    {
      "scale": "x",
      "orient": "bottom",
      "title": "x",
      "labelAlign": "right",
      "labelBaseline": "middle",
      "grid": false,
      "labelAngle": 270,
      "zindex": 0
    },
    {
      "scale": "y",
      "title": "Median of y",
      "zindex": 0,
      "labelOverlap": true,
      "tickCount": {
        "signal": "ceil(height/40)"
      },
      "orient": "left",
      "grid": false
    }
  ],
  "padding": 5,
  "background": "white",
  "autosize": {
    "type": "fit",
    "contains": "padding"
  },
  "config": {
    "customFormatTypes": true,
    "style": {
      "guide-label": {
        "font": "'IBM Plex Sans', system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
      },
      "guide-title": {
        "font": "'IBM Plex Sans', system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
      },
      "group-title": {
        "font": "'IBM Plex Sans', system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
      },
      "group-subtitle": {
        "font": "'IBM Plex Sans', system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
      },
      "cell": {},
      "text": {
        "font": "'IBM Plex Sans', system-ui, -apple-system, BlinkMacSystemFont, sans-serif"
      }
    }
  },
  "style": "cell",
  "usermeta": {
    "warnings": []
  }
}    
    """


def date_column_spec():
    return r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "description": "A scatterplot showing horsepower and miles per gallons for various cars.",
  "background": "white",
  "padding": 5,
  "width": 400,
  "height": 20,
  "style": "cell",
  "data": [
    {
      "name": "data_0",
      "url": "vegafusion+dataset://dates",
      "transform": [
        {
          "type": "formula",
          "expr": "toDate(datum[\"date_col\"])",
          "as": "date_col"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "marks",
      "type": "symbol",
      "style": ["point"],
      "from": {"data": "data_0"},
      "encode": {
        "update": {
          "opacity": {"value": 0.7},
          "fill": {"value": "transparent"},
          "stroke": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "point"},
          "description": {
            "signal": "\"date32: \" + (timeFormat(datum[\"date32\"], '%b %d, %Y'))"
          },
          "x": {"scale": "x", "field": "date_col"},
          "y": {"signal": "height", "mult": 0.5}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "time",
      "domain": {"data": "data_0", "field": "date_col"},
      "range": [0, {"signal": "width"}]
    }
  ]
}
"""


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


def test_pre_transform_planner_warning1():
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


def test_pre_transform_planner_warning2():
    # Pre-transform with supported aggregate function should result in no warnings
    vega_spec = standalone_aggregate_spec("mean")
    datasets, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC")
    assert len(warnings) == 0

    # Pre-transform with unsupported aggregate function should result in one warning
    vega_spec = standalone_aggregate_spec("ci0")
    datasets, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC")
    assert len(warnings) == 1

    warning = warnings[0]
    assert warning["type"] == "Planner"
    assert "data_0" in warning["message"]


def test_date32_pre_transform_dataset():
    # Test to make sure that date32 columns are interpreted in the local timezone
    dates_df = pd.DataFrame({
        "date_col": [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
    })
    spec = date_column_spec()

    (output_ds,), warnings = vf.runtime.pre_transform_datasets(
        spec, ["data_0"], "America/New_York", default_input_tz="UTC", inline_datasets=dict(dates=dates_df)
    )

    assert list(output_ds.date_col) == [
        "2022-01-01T00:00:00.000", "2022-01-02T00:00:00.000", "2022-01-03T00:00:00.000"
    ]
