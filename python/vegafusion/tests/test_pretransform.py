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

