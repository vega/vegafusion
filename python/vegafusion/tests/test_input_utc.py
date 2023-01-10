import vegafusion as vf
import pandas as pd
import json


def test_input_utc():
    (pre_transformed, warnings) = vf.runtime.pre_transform_spec(
        input_spec(), "UTC", "UTC", inline_datasets={"seattle_weather": load_dataset()}
    )
    print(json.dumps(pre_transformed, indent=2))
    expected = expected_spec()
    assert pre_transformed == expected


def load_dataset():
    """
    Load seattle-weather dataset with the date column localized as UTC
    """
    seattle_weather = pd.read_csv("https://raw.githubusercontent.com/vega/vega-datasets/next/data/seattle-weather.csv")
    seattle_weather["date"] = pd.to_datetime(seattle_weather["date"])
    seattle_weather = seattle_weather.set_index("date").tz_localize("UTC").reset_index()
    return seattle_weather


def input_spec():
    return r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "source_0",
      "url": "vegafusion+dataset://seattle_weather",
      "format": {"type": "csv", "parse": {"date": "date"}},
      "transform": [
        {
          "field": "date",
          "type": "timeunit",
          "units": ["month"],
          "as": ["month_date", "month_date_end"]
        },
        {
          "type": "aggregate",
          "groupby": ["month_date", "weather"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "stack",
          "groupby": ["month_date"],
          "field": "__count",
          "sort": {"field": ["weather"], "order": ["descending"]},
          "as": ["__count_start", "__count_end"],
          "offset": "zero"
        },
        {
            "type": "collect",
            "sort": {"field": ["month_date", "weather"]}
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
      "name": "marks",
      "type": "rect",
      "style": ["bar"],
      "from": {"data": "source_0"},
      "encode": {
        "update": {
          "fill": {"scale": "color", "field": "weather"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"Month of the year: \" + (timeFormat(datum[\"month_date\"], timeUnitSpecifier([\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"}))) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\")) + \"; Weather type: \" + (isValid(datum[\"weather\"]) ? datum[\"weather\"] : \"\"+datum[\"weather\"])"
          },
          "x": {"scale": "x", "field": "month_date"},
          "width": {"signal": "max(0.25, bandwidth('x'))"},
          "y": {"scale": "y", "field": "__count_end"},
          "y2": {"scale": "y", "field": "__count_start"}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "band",
      "domain": {"data": "source_0", "field": "month_date", "sort": true},
      "range": {"step": {"signal": "x_step"}},
      "paddingInner": 0.1,
      "paddingOuter": 0.05
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {
        "data": "source_0",
        "fields": ["__count_start", "__count_end"]
      },
      "range": [{"signal": "height"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "color",
      "type": "ordinal",
      "domain": ["sun", "fog", "drizzle", "rain", "snow"],
      "range": ["#e7ba52", "#c7c7c7", "#aec7e8", "#1f77b4", "#9467bd"]
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
      "title": "Month of the year",
      "format": {
        "signal": "timeUnitSpecifier([\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"})"
      },
      "formatType": "time",
      "labelOverlap": true,
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "Count of Records",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ],
  "legends": [
    {"title": "Weather type", "fill": "color", "symbolType": "square"}
  ]
}
"""


def expected_spec():
    return json.loads(r"""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "source_0",
      "values": [
        {
          "__count": 10,
          "__count_end": 124,
          "__count_start": 114,
          "month_date": "2012-01-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 17,
          "__count_end": 114,
          "__count_start": 97,
          "month_date": "2012-01-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 61,
          "__count_end": 97,
          "__count_start": 36,
          "month_date": "2012-01-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 8,
          "__count_end": 36,
          "__count_start": 28,
          "month_date": "2012-01-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 28,
          "__count_end": 28,
          "__count_start": 0,
          "month_date": "2012-01-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 4,
          "__count_end": 113,
          "__count_start": 109,
          "month_date": "2012-02-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 3,
          "__count_end": 109,
          "__count_start": 106,
          "month_date": "2012-02-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 75,
          "__count_end": 106,
          "__count_start": 31,
          "month_date": "2012-02-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 4,
          "__count_end": 31,
          "__count_start": 27,
          "month_date": "2012-02-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 27,
          "__count_end": 27,
          "__count_start": 0,
          "month_date": "2012-02-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 3,
          "__count_end": 124,
          "__count_start": 121,
          "month_date": "2012-03-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 6,
          "__count_end": 121,
          "__count_start": 115,
          "month_date": "2012-03-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 73,
          "__count_end": 115,
          "__count_start": 42,
          "month_date": "2012-03-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 6,
          "__count_end": 42,
          "__count_start": 36,
          "month_date": "2012-03-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 36,
          "__count_end": 36,
          "__count_start": 0,
          "month_date": "2012-03-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 3,
          "__count_end": 120,
          "__count_start": 117,
          "month_date": "2012-04-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 3,
          "__count_end": 117,
          "__count_start": 114,
          "month_date": "2012-04-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 61,
          "__count_end": 114,
          "__count_start": 53,
          "month_date": "2012-04-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 1,
          "__count_end": 53,
          "__count_start": 52,
          "month_date": "2012-04-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 52,
          "__count_end": 52,
          "__count_start": 0,
          "month_date": "2012-04-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 1,
          "__count_end": 124,
          "__count_start": 123,
          "month_date": "2012-05-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 5,
          "__count_end": 123,
          "__count_start": 118,
          "month_date": "2012-05-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 40,
          "__count_end": 118,
          "__count_start": 78,
          "month_date": "2012-05-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 78,
          "__count_end": 78,
          "__count_start": 0,
          "month_date": "2012-05-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 2,
          "__count_end": 120,
          "__count_start": 118,
          "month_date": "2012-06-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 1,
          "__count_end": 118,
          "__count_start": 117,
          "month_date": "2012-06-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 42,
          "__count_end": 117,
          "__count_start": 75,
          "month_date": "2012-06-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 75,
          "__count_end": 75,
          "__count_start": 0,
          "month_date": "2012-06-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 8,
          "__count_end": 124,
          "__count_start": 116,
          "month_date": "2012-07-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 10,
          "__count_end": 116,
          "__count_start": 106,
          "month_date": "2012-07-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 16,
          "__count_end": 106,
          "__count_start": 90,
          "month_date": "2012-07-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 90,
          "__count_end": 90,
          "__count_start": 0,
          "month_date": "2012-07-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 8,
          "__count_end": 124,
          "__count_start": 116,
          "month_date": "2012-08-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 6,
          "__count_end": 116,
          "__count_start": 110,
          "month_date": "2012-08-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 24,
          "__count_end": 110,
          "__count_start": 86,
          "month_date": "2012-08-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 86,
          "__count_end": 86,
          "__count_start": 0,
          "month_date": "2012-08-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 5,
          "__count_end": 120,
          "__count_start": 115,
          "month_date": "2012-09-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 14,
          "__count_end": 115,
          "__count_start": 101,
          "month_date": "2012-09-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 36,
          "__count_end": 101,
          "__count_start": 65,
          "month_date": "2012-09-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 65,
          "__count_end": 65,
          "__count_start": 0,
          "month_date": "2012-09-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 4,
          "__count_end": 124,
          "__count_start": 120,
          "month_date": "2012-10-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 19,
          "__count_end": 120,
          "__count_start": 101,
          "month_date": "2012-10-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 62,
          "__count_end": 101,
          "__count_start": 39,
          "month_date": "2012-10-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 39,
          "__count_end": 39,
          "__count_start": 0,
          "month_date": "2012-10-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 3,
          "__count_end": 120,
          "__count_start": 117,
          "month_date": "2012-11-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 9,
          "__count_end": 117,
          "__count_start": 108,
          "month_date": "2012-11-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 75,
          "__count_end": 108,
          "__count_start": 33,
          "month_date": "2012-11-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 1,
          "__count_end": 33,
          "__count_start": 32,
          "month_date": "2012-11-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 32,
          "__count_end": 32,
          "__count_start": 0,
          "month_date": "2012-11-01T00:00:00.000",
          "weather": "sun"
        },
        {
          "__count": 2,
          "__count_end": 124,
          "__count_start": 122,
          "month_date": "2012-12-01T00:00:00.000",
          "weather": "drizzle"
        },
        {
          "__count": 8,
          "__count_end": 122,
          "__count_start": 114,
          "month_date": "2012-12-01T00:00:00.000",
          "weather": "fog"
        },
        {
          "__count": 76,
          "__count_end": 114,
          "__count_start": 38,
          "month_date": "2012-12-01T00:00:00.000",
          "weather": "rain"
        },
        {
          "__count": 6,
          "__count_end": 38,
          "__count_start": 32,
          "month_date": "2012-12-01T00:00:00.000",
          "weather": "snow"
        },
        {
          "__count": 32,
          "__count_end": 32,
          "__count_start": 0,
          "month_date": "2012-12-01T00:00:00.000",
          "weather": "sun"
        }
      ],
      "transform": [
        {
          "type": "formula",
          "expr": "toDate(datum['month_date'])",
          "as": "month_date"
        }
      ]
    },
    {
      "name": "source_0_x_domain_month_date",
      "values": [
        {"month_date": "2012-01-01T00:00:00.000"},
        {"month_date": "2012-02-01T00:00:00.000"},
        {"month_date": "2012-03-01T00:00:00.000"},
        {"month_date": "2012-04-01T00:00:00.000"},
        {"month_date": "2012-05-01T00:00:00.000"},
        {"month_date": "2012-06-01T00:00:00.000"},
        {"month_date": "2012-07-01T00:00:00.000"},
        {"month_date": "2012-08-01T00:00:00.000"},
        {"month_date": "2012-09-01T00:00:00.000"},
        {"month_date": "2012-10-01T00:00:00.000"},
        {"month_date": "2012-11-01T00:00:00.000"},
        {"month_date": "2012-12-01T00:00:00.000"}
      ],
      "transform": [
        {
          "type": "formula",
          "expr": "toDate(datum['month_date'])",
          "as": "month_date"
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
      "type": "rect",
      "name": "marks",
      "from": {"data": "source_0"},
      "encode": {
        "update": {
          "x": {"field": "month_date", "scale": "x"},
          "description": {
            "signal": "\"Month of the year: \" + (timeFormat(datum[\"month_date\"], timeUnitSpecifier([\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"}))) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\")) + \"; Weather type: \" + (isValid(datum[\"weather\"]) ? datum[\"weather\"] : \"\"+datum[\"weather\"])"
          },
          "fill": {"field": "weather", "scale": "color"},
          "ariaRoleDescription": {"value": "bar"},
          "width": {"signal": "max(0.25, bandwidth('x'))"},
          "y": {"field": "__count_end", "scale": "y"},
          "y2": {"field": "__count_start", "scale": "y"}
        }
      },
      "style": ["bar"]
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "band",
      "domain": {
        "data": "source_0_x_domain_month_date",
        "field": "month_date",
        "sort": true
      },
      "range": {"step": {"signal": "x_step"}},
      "paddingInner": 0.1,
      "paddingOuter": 0.05
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {
        "fields": ["__count_start", "__count_end"],
        "data": "source_0"
      },
      "range": [{"signal": "height"}, 0],
      "zero": true,
      "nice": true
    },
    {
      "name": "color",
      "type": "ordinal",
      "domain": ["sun", "fog", "drizzle", "rain", "snow"],
      "range": ["#e7ba52", "#c7c7c7", "#aec7e8", "#1f77b4", "#9467bd"]
    }
  ],
  "axes": [
    {
      "scale": "y",
      "domain": false,
      "minExtent": 0,
      "aria": false,
      "orient": "left",
      "tickCount": {"signal": "ceil(height/40)"},
      "grid": true,
      "labels": false,
      "maxExtent": 0,
      "ticks": false,
      "gridScale": "x",
      "zindex": 0
    },
    {
      "scale": "x",
      "formatType": "time",
      "grid": false,
      "orient": "bottom",
      "title": "Month of the year",
      "format": {
        "signal": "timeUnitSpecifier([\"month\"], {\"year-month\":\"%b %Y \",\"year-month-date\":\"%b %d, %Y \"})"
      },
      "labelOverlap": true,
      "zindex": 0
    },
    {
      "scale": "y",
      "labelOverlap": true,
      "orient": "left",
      "title": "Count of Records",
      "grid": false,
      "zindex": 0,
      "tickCount": {"signal": "ceil(height/40)"}
    }
  ],
  "padding": 5,
  "background": "white",
  "legends": [
    {"title": "Weather type", "fill": "color", "symbolType": "square"}
  ],
  "style": "cell",
  "height": 200
}
    """)
