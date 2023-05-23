import pandas as pd
import pytest
from pandas import Timestamp, NaT
import vegafusion as vf
import json
import polars as pl
from datetime import date
import decimal


def get_connections():
    connections = ["datafusion"]
    try:
        import duckdb
        connections.append("duckdb")
    except ImportError:
        pass

    return connections


def order_items_spec():
    return json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "height": 200,
  "style": "cell",
  "data": [
    {"name": "order_items", "url": "table://order_items"},
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
        },
        {
            "type": "collect",
            "sort": {"field": ["menu_item"]}
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
""")


def movies_histogram_spec(agg="count"):
    return json.loads("""
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
}""")


def standalone_aggregate_spec(agg="count"):
    return json.loads("""
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
    """)


def date_column_spec():
    return json.loads(r"""
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
""")


def period_in_col_name_spec():
    return json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "width": 200,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "df_period",
      "url": "vegafusion+dataset://df_period"
    },
    {
      "name": "data_0",
      "source": "df_period",
      "transform": [
        {
          "type": "filter",
          "expr": "isValid(datum[\"normal\"]) && isFinite(+datum[\"normal\"]) && isValid(datum[\"a.b\"]) && isFinite(+datum[\"a.b\"])"
        }
      ]
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
          "tooltip": {
            "signal": "{\"normal\": format(datum[\"normal\"], \"\"), \"a\\.b\": format(datum[\"a.b\"], \"\")}"
          },
          "fill": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"normal: \" + (format(datum[\"normal\"], \"\")) + \"; a\\.b: \" + (format(datum[\"a.b\"], \"\"))"
          },
          "xc": {"scale": "x", "field": "normal"},
          "width": {"value": 5},
          "y": {"scale": "y", "field": "a\\.b"},
          "y2": {"scale": "y", "value": 0}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "linear",
      "domain": {"data": "data_0", "field": "normal"},
      "range": [0, {"signal": "width"}],
      "nice": true,
      "zero": false,
      "padding": 5
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "data_0", "field": "a\\.b"},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    }
  ],
  "axes": [
    {
      "scale": "x",
      "orient": "bottom",
      "gridScale": "y",
      "grid": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "domain": false,
      "labels": false,
      "aria": false,
      "maxExtent": 0,
      "minExtent": 0,
      "ticks": false,
      "zindex": 0
    },
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
      "title": "normal",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "a\\.b",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ]
} 
    """)


def nat_bar_spec():
    return json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "width": 200,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "dataframe",
      "url": "vegafusion+dataset://dataframe",
      "format": {"type": "json", "parse": {"NULL_TEST": "date"}},
      "transform": [
        {
          "type": "stack",
          "groupby": ["NULL_TEST"],
          "field": "SALES",
          "sort": {"field": [], "order": []},
          "as": ["SALES_start", "SALES_end"],
          "offset": "zero"
        },
        {
          "type": "filter",
          "expr": "(isDate(datum[\"NULL_TEST\"]) || (isValid(datum[\"NULL_TEST\"]) && isFinite(+datum[\"NULL_TEST\"]))) && isValid(datum[\"SALES\"]) && isFinite(+datum[\"SALES\"])"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "layer_0_marks",
      "type": "rect",
      "clip": true,
      "style": ["bar"],
      "from": {"data": "dataframe"},
      "encode": {
        "update": {
          "tooltip": {
            "signal": "{\"NULL_TEST\": timeFormat(datum[\"NULL_TEST\"], '%b %d, %Y'), \"SALES\": format(datum[\"SALES\"], \"\")}"
          },
          "fill": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"NULL_TEST: \" + (timeFormat(datum[\"NULL_TEST\"], '%b %d, %Y')) + \"; SALES: \" + (format(datum[\"SALES\"], \"\"))"
          },
          "xc": {"scale": "x", "field": "NULL_TEST"},
          "width": {"value": 5},
          "y": {"scale": "y", "field": "SALES_end"},
          "y2": {"scale": "y", "field": "SALES_start"}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "time",
      "domain": {"data": "dataframe", "field": "NULL_TEST"},
      "range": [0, {"signal": "width"}],
      "padding": 5
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "dataframe", "fields": ["SALES_start", "SALES_end"]},
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    }
  ],
  "axes": [
    {
      "scale": "x",
      "orient": "bottom",
      "gridScale": "y",
      "grid": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "domain": false,
      "labels": false,
      "aria": false,
      "maxExtent": 0,
      "minExtent": 0,
      "ticks": false,
      "zindex": 0
    },
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
      "title": "NULL_TEST",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "SALES",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ],
  "config": {
    "range": {"ramp": {"scheme": "yellowgreenblue"}},
    "axis": {"domain": false}
  }
}
    """)


def date32_timeunit_spec():
    return json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "autosize": {
    "type": "fit",
    "contains": "padding"
  },
  "background": "white",
  "padding": 5,
  "style": "cell",
  "data": [
    {
      "name": "dataframe",
      "url": "vegafusion+dataset://dataframe"
    },
    {
      "name": "data_0",
      "source": "dataframe",
      "transform": [
        {
          "type": "filter",
          "expr": "isValid(datum[\"GO_LIVE_MONTH\"])"
        },
        {
          "field": "GO_LIVE_MONTH",
          "type": "timeunit",
          "units": [
            "year",
            "month"
          ],
          "as": [
            "go_live_start",
            "go_live_end"
          ]
        },
        {
          "type": "stack",
          "groupby": [
            "go_live_start"
          ],
          "field": "PERCENT_GO_LIVES",
          "sort": {
            "field": [],
            "order": []
          },
          "as": [
            "percent_go_lives_start",
            "percent_go_lives_end"
          ],
          "offset": "normalize"
        },
        {
          "type": "formula",
          "expr": "datum['percent_go_lives_end']-datum['percent_go_lives_start']",
          "as": "percent_go_lives_delta"
        }
      ]
    },
    {
      "name": "data_1",
      "source": "data_0",
      "transform": [
        {
          "field": "go_live_start",
          "type": "timeunit",
          "units": [
            "year",
            "month"
          ],
          "as": [
            "go_live_start_start",
            "go_live_start_end"
          ]
        },
        {
          "type": "filter",
          "expr": "(isDate(datum[\"go_live_start_start\"]) || (isValid(datum[\"go_live_start_start\"]) && isFinite(+datum[\"go_live_start_start\"]))) && isValid(datum[\"percent_go_lives_start\"]) && isFinite(+datum[\"percent_go_lives_start\"])"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "layer_0_layer_0_layer_0_marks",
      "type": "rect",
      "clip": true,
      "style": [
        "bar"
      ],
      "from": {
        "data": "data_1"
      },
      "encode": {
        "update": {
          "fill": {
            "value": "#ff4c00"
          },
          "opacity": {
            "value": 1
          },
          "ariaRoleDescription": {
            "value": "bar"
          },
          "x": {
            "scale": "x",
            "field": "go_live_start_start"
          },
          "x2": {
            "scale": "x",
            "field": "go_live_end",
            "offset": -1
          },
          "y": {
            "scale": "y",
            "field": "percent_go_lives_start"
          },
          "y2": {
            "scale": "y",
            "field": "percent_go_lives_end"
          }
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "time",
      "domain": {
        "fields": [
          {
            "data": "data_1",
            "field": "go_live_start_start"
          },
          {
            "data": "data_1",
            "field": "go_live_end"
          }
        ]
      },
      "range": [
        0,
        {
          "signal": "width"
        }
      ]
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {
        "fields": [
          {
            "data": "data_1",
            "field": "percent_go_lives_start"
          },
          {
            "data": "data_1",
            "field": "percent_go_lives_end"
          }
        ]
      },
      "range": [
        {
          "signal": "height"
        },
        0
      ],
      "nice": true,
      "zero": true
    }
  ]
}

""")
def test_pre_transform_multi_partition():
    n = 4050
    order_items = pd.DataFrame({
        "menu_item": [0] * n + [1] * n
    })

    vega_spec = order_items_spec()
    new_spec, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC", inline_datasets={
        "order_items": order_items,
    })

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

    result = datasets[0]
    expected = pd.DataFrame({"menu_item": [0, 1, 2], "__count": [n, 2 * n, 3 * n]})
    pd.testing.assert_frame_equal(result, expected)


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

    # Timestamps are in the local timezone, so they should be midnight local time
    assert list(output_ds.date_col) == [
        pd.Timestamp('2022-01-01 00:00:00-0500', tz='America/New_York'),
        pd.Timestamp('2022-01-02 00:00:00-0500', tz='America/New_York'),
        pd.Timestamp('2022-01-03 00:00:00-0500', tz='America/New_York')
    ]


def test_date32_in_timeunit_duckdb_crash():
    try:
        # Set this as the active connection
        vf.runtime.set_connection("duckdb")

        # order_items includes a table://order_items data url
        vega_spec = date32_timeunit_spec()
        dataframe = pd.DataFrame({
            "GO_LIVE_MONTH": [date(2021, 1, 1), date(2021, 2, 1)],
            "PERCENT_GO_LIVES": [0.2, 0.3],
        })

        datasets, warnings = vf.runtime.pre_transform_datasets(
            vega_spec,
            ["data_1"],
            "UTC",
            inline_datasets=dict(dataframe=dataframe)
        )
        assert len(warnings) == 0
        assert len(datasets) == 1
        assert len(datasets[0]) == 2
    finally:
        vf.runtime.set_connection("datafusion")


def test_period_in_column_name():
    df_period = pd.DataFrame([[1, 2]], columns=['normal', 'a.b'])
    spec = period_in_col_name_spec()
    datasets, warnings = vf.runtime.pre_transform_datasets(spec, ["data_0"], "UTC", inline_datasets=dict(
        df_period=df_period
    ))
    assert len(warnings) == 0
    assert len(datasets) == 1

    dataset = datasets[0]
    assert dataset.to_dict("records") == [{"normal": 1, "a.b": 2}]


def test_nat_values():
    dataframe = pd.DataFrame([
        {'ORDER_DATE': date(2011, 3, 1),
         'SALES': 457.568,
         'NULL_TEST': Timestamp('2011-03-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 3, 1),
         'SALES': 376.509,
         'NULL_TEST': Timestamp('2011-03-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 3, 1),
         'SALES': 362.25,
         'NULL_TEST': Timestamp('2011-03-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 3, 1),
         'SALES': 129.552,
         'NULL_TEST': Timestamp('2011-03-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 3, 1), 'SALES': 18.84, 'NULL_TEST': NaT},
        {'ORDER_DATE': date(2011, 4, 1),
         'SALES': 66.96,
         'NULL_TEST': Timestamp('2011-04-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 4, 1), 'SALES': 6.24, 'NULL_TEST': NaT},
        {'ORDER_DATE': date(2011, 6, 1),
         'SALES': 881.93,
         'NULL_TEST': Timestamp('2011-06-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 6, 1),
         'SALES': 166.72,
         'NULL_TEST': Timestamp('2011-06-01 00:00:00+0000', tz="UTC")},
        {'ORDER_DATE': date(2011, 6, 1), 'SALES': 25.92, 'NULL_TEST': NaT}
    ])

    spec = nat_bar_spec()

    datasets, warnings = vf.runtime.pre_transform_datasets(spec, ["dataframe"], "UTC", inline_datasets=dict(
        dataframe=dataframe
    ))
    assert len(warnings) == 0
    assert len(datasets) == 1

    dataset = datasets[0]
    assert dataset.to_dict("records")[0] == {
        'NULL_TEST': pd.Timestamp('2011-03-01 00:00:00+0000', tz="UTC"),
        'ORDER_DATE': Timestamp('2011-03-01 00:00:00+0000', tz="UTC"),
        'SALES': 457.568,
        'SALES_end': 457.568,
        'SALES_start': 0.0,
    }


def test_pre_transform_dataset_dataframe_interface_protocol():
    try:
        import pyarrow.interchange
    except ImportError:
        pytest.skip("DataFrame interface protocol requires pyarrow 11.0.0 or later")

    n = 4050
    # Input a polars DataFrame (which follows the DataFrame Interface Protocol)
    order_items = pl.DataFrame({
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

    result = datasets[0]

    # Result should be a polars DataFrame
    assert isinstance(result, pl.DataFrame)
    expected = pl.DataFrame({"menu_item": [0, 1, 2], "__count": [n, 2 * n, 3 * n]})
    assert result.frame_equal(expected)


def test_pre_transform_dataset_duckdb_conn():
    import duckdb

    n = 4050
    # Input a polars DataFrame (which follows the DataFrame Interface Protocol)
    order_items = pd.DataFrame({
        "menu_item": [0] * n + [1] * (2 * n) + [2] * (3 * n)
    })

    try:
        # Create duckdb connection and register order_items with duckdb
        conn = duckdb.connect()
        conn.register("order_items", order_items)

        # Set this as the active connection
        vf.runtime.set_connection(conn)

        # order_items includes a table://order_items data url
        vega_spec = order_items_spec()
        datasets, warnings = vf.runtime.pre_transform_datasets(
            vega_spec,
            ["data_0"],
            "UTC",
        )
        assert len(warnings) == 0
        assert len(datasets) == 1

        result = datasets[0]
        expected = pd.DataFrame({"menu_item": [0, 1, 2], "__count": [n, 2 * n, 3 * n]})
        pd.testing.assert_frame_equal(result, expected)
    finally:
        vf.runtime.set_connection("datafusion")


def test_pre_transform_dataset_duckdb_with_decimal_conn():
    import duckdb

    n = 4050
    # Input a polars DataFrame (which follows the DataFrame Interface Protocol)
    order_items = pd.DataFrame({
        "menu_item_int": [0] * n + [1] * (2 * n) + [2] * (3 * n)
    })

    try:
        # Create duckdb connection and register order_items with duckdb
        conn = duckdb.connect()
        conn.register("order_items_int", order_items)
        conn.query(
            "SELECT menu_item_int::DECIMAL(12,2) as menu_item from order_items_int"
        ).to_view("order_items")

        # Set this as the active connection
        vf.runtime.set_connection(conn)

        # order_items includes a table://order_items data url
        vega_spec = order_items_spec()
        datasets, warnings = vf.runtime.pre_transform_datasets(
            vega_spec,
            ["data_0"],
            "UTC",
        )
        assert len(warnings) == 0
        assert len(datasets) == 1

        result = datasets[0]
        expected = pd.DataFrame({
            "menu_item": [decimal.Decimal(0), decimal.Decimal(1), decimal.Decimal(2)],
            "__count": [n, 2 * n, 3 * n]
        })
        pd.testing.assert_frame_equal(result, expected)
    finally:
        vf.runtime.set_connection("datafusion")


def test_duckdb_timestamp_with_timezone():
    try:
        vf.runtime.set_connection("duckdb")
        dates_df = pd.DataFrame({
            "date_col": [date(2022, 1, 1), date(2022, 1, 2), date(2022, 1, 3)],
        })
        dates_df["date_col"] = pd.to_datetime(dates_df.date_col).dt.tz_localize("UTC")
        spec = date_column_spec()

        (output_ds,), warnings = vf.runtime.pre_transform_datasets(
            spec, ["data_0"], "America/New_York", default_input_tz="UTC", inline_datasets=dict(dates=dates_df)
        )

        # Timestamps are in the local timezone, so they should be midnight local time
        assert list(output_ds.date_col) == [
            pd.Timestamp('2022-01-01 00:00:00', tz='UTC'),
            pd.Timestamp('2022-01-02 00:00:00', tz='UTC'),
            pd.Timestamp('2022-01-03 00:00:00', tz='UTC')
        ]
    finally:
        vf.runtime.set_connection("datafusion")


def test_gh_268_hang():
    """
    Tests for hang reported in https://github.com/hex-inc/vegafusion/issues/268
    """
    vf.runtime.set_connection("datafusion")
    movies = pd.read_json("https://raw.githubusercontent.com/vega/vega-datasets/main/data/movies.json")
    spec = gh_268_hang_spec()
    for i in range(20):
        # Break cache by removing one row each iteration
        movies_inner = movies.iloc[i:]
        vf.runtime.pre_transform_datasets(spec, ["data_3"], "UTC", inline_datasets=dict(movies_clean=movies_inner))


def test_repeat_duckdb():
    """
    Tests for hang reported in https://github.com/hex-inc/vegafusion/issues/268
    """
    vf.runtime.set_connection("duckdb")
    movies = pd.read_json("https://raw.githubusercontent.com/vega/vega-datasets/main/data/movies.json")
    spec = gh_268_hang_spec()
    for i in range(2):
        vf.runtime.pre_transform_datasets(spec, ["data_3"], "UTC", inline_datasets=dict(movies_clean=movies))


@pytest.mark.parametrize("connection", get_connections())
def test_pivot_mixed_case(connection):
    vf.runtime.set_connection(connection)
    source_0 = pd.DataFrame.from_records([
        {"country": "Norway", "type": "gold", "count": 14},
        {"country": "Norway", "type": "silver", "count": 14},
        {"country": "Norway", "type": "Gold", "count": 11},
        {"country": "Germany", "type": "gold", "count": 14},
        {"country": "Germany", "type": "silver", "count": 10},
        {"country": "Germany", "type": "bronze", "count": 7},
        {"country": "Canada", "type": "gold", "count": 11},
        {"country": "Canada", "type": "silver", "count": 8},
        {"country": "Canada", "type": "bronze", "count": 10}
    ])
    spec = json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "source_0",
      "url": "table://source_0"
    },
    {
      "name": "data_0",
      "source": "source_0",
      "transform": [
        {
          "type": "pivot",
          "field": "type",
          "value": "count",
          "groupby": ["country"]
        }
      ]
    }
  ]
}   
    """)

    datasets, warnings = vf.runtime.pre_transform_datasets(
        spec, ["data_0"], "UTC", inline_datasets=dict(source_0=source_0)
    )

    assert set(datasets[0].columns.tolist()) == {"gold", "Gold", "silver", "bronze", "country"}


def gh_268_hang_spec():
    return json.loads(r""" 
    {
      "$schema": "https://vega.github.io/schema/vega/v5.json",
      "data": [
        {
          "name": "interval_intervalselection__store"
        },
        {
          "name": "legend_pointselection__store"
        },
        {
          "name": "pivot_hover_32f2e9aa_f08a_4fb5_aa8a_ab3f2cc94a1d_store"
        },
        {
          "name": "movies_clean",
          "url": "vegafusion+dataset://movies_clean"
        },
        {
          "name": "data_0",
          "source": "movies_clean",
          "transform": [
            {
              "type": "formula",
              "expr": "toDate(datum[\"Release Date\"])",
              "as": "Release Date"
            }
          ]
        },
        {
          "name": "data_2",
          "source": "data_0",
          "transform": [
            {
              "field": "Release Date",
              "type": "timeunit",
              "units": [
                "year"
              ],
              "as": [
                "year_Release Date",
                "year_Release Date_end"
              ]
            }
          ]
        },
        {
          "name": "data_3",
          "source": "data_2",
          "transform": [
            {
              "type": "filter",
              "expr": "!length(data(\"interval_intervalselection__store\")) || vlSelectionTest(\"interval_intervalselection__store\", datum)"
            },
            {
              "type": "filter",
              "expr": "time('1986-11-09T18:28:05.617') <= time(datum[\"year_Release Date\"]) && time(datum[\"year_Release Date\"]) <= time('2001-09-16T22:23:39.144')"
            },
            {
              "type": "filter",
              "expr": "!length(data(\"legend_pointselection__store\")) || vlSelectionTest(\"legend_pointselection__store\", datum)"
            }
          ]
        }
      ]
    }
    """)