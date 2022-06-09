import pandas as pd
import vegafusion as vf
import json


def test_pre_transform_multi_partition():
    n = 4050
    order_items = pd.DataFrame({
        "menu_item": [0] * n + [1] * n
    })

    vega_spec = r"""
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
    new_spec, warnings = vf.runtime.pre_transform_spec(vega_spec, "UTC", inline_datasets={
        "order_items": order_items,
    })
    new_spec = json.loads(new_spec)
    data0 = new_spec["data"][1]
    datum0 = data0["values"][0]
    datum1 = data0["values"][1]

    # Check for expected counts in data_0 dataset
    assert data0["name"] == "data_0"
    assert datum0["menu_item"] == 0
    assert datum0["__count"] == n
    assert datum1["menu_item"] == 1
    assert datum1["__count"] == n
