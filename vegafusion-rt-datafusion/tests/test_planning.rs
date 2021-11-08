use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::planning::extract::extract_server_data;
use vegafusion_core::proto::gen::tasks::{TaskGraph, Variable};
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
use std::sync::Arc;
use std::collections::HashSet;
use vegafusion_core::planning::stitch::stitch_specs;

#[tokio::test(flavor="multi_thread")]
async fn test_extract_server_data() {
    let mut spec = spec1();

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();
    // println!("{:#?}", task_scope);

    let server_spec = extract_server_data(&mut spec, &mut task_scope).unwrap();
    // println!("{}", serde_json::to_string_pretty(&server_spec).unwrap());

    let client_defs: HashSet<_> = spec.definition_vars().unwrap().into_iter().collect();
    let client_inputs: HashSet<_> = spec.input_vars(&task_scope).unwrap().into_iter().collect();
    let client_updates: HashSet<_> = spec.update_vars(&task_scope).unwrap().into_iter().collect();

    let server_defs: HashSet<_> = server_spec.definition_vars().unwrap().into_iter().collect();
    let server_inputs: HashSet<_> = server_spec.input_vars(&task_scope).unwrap().into_iter().collect();
    let server_updates: HashSet<_> = server_spec.update_vars(&task_scope).unwrap().into_iter().collect();

    let client_to_server: Vec<_> = client_updates.intersection(&server_inputs).collect();
    println!("client_to_server: {:?}", client_to_server);

    let server_to_client: Vec<_> = server_updates.intersection(&client_inputs).collect();
    println!("server_to_client: {:?}", server_to_client);

    let server_stubs: Vec<_> = server_inputs.difference(&server_defs).collect();
    println!("server_stubs: {:?}", server_stubs);

    let client_stubs: Vec<_> = client_inputs.difference(&client_defs).collect();
    println!("client_stubs: {:?}", client_stubs);

    let tasks = server_spec.to_tasks().unwrap();
    let graph = Arc::new(TaskGraph::new(tasks, &task_scope).unwrap());
    let mapping = graph.build_mapping();
    // println!("{:#?}", mapping);

    let graph_runtime = TaskGraphRuntime::new(20);
    let data_3 = graph_runtime.get_node_value(
        graph.clone(), mapping.get(&(Variable::new_data("data_3"), Vec::new())).unwrap()
    ).await.unwrap();

    // println!("data_3:\n{}", data_3.into_table().unwrap().pretty_format(None).unwrap());

    let delay_extent = graph_runtime.get_node_value(
        graph.clone(),
        mapping.get(&(Variable::new_signal("child__column_delay_layer_1_bin_maxbins_20_delay_extent"), Vec::new())).unwrap()
    ).await.unwrap();

    // println!("delay_extent: {:?}", delay_extent.into_scalar().unwrap())
}

#[tokio::test(flavor="multi_thread")]
async fn test_extract_stitch_data() {
    let mut spec = spec1();

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();

    let mut server_spec = extract_server_data(&mut spec, &mut task_scope).unwrap();
    let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut spec).unwrap();

    println!("{:#?}", comm_plan);

    println!("client spec:\n{}", serde_json::to_string_pretty(&spec).unwrap());
}

#[tokio::test(flavor="multi_thread")]
async fn try_extract_split_server_data() {
    let mut spec = weather_spec();

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();

    let mut server_spec = extract_server_data(&mut spec, &mut task_scope).unwrap();
    let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut spec).unwrap();

    println!("{:#?}", comm_plan);

    println!("server spec:\n{}\n\n", serde_json::to_string_pretty(&server_spec).unwrap());
    println!("client spec:\n{}\n\n", serde_json::to_string_pretty(&spec).unwrap());
}


fn spec1() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "data": [
    {"name": "brush_store"},
    {
      "name": "source_0",
      "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/flights-2k.json",
      "format": {"type": "json", "parse": {"date": "date"}},
      "transform": [
        {
          "type": "extent",
          "field": "delay",
          "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_extent"
        },
        {
          "type": "bin",
          "field": "delay",
          "as": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_bins",
          "extent": {
            "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_extent"
          },
          "maxbins": 20
        },
        {
          "type": "extent",
          "field": "distance",
          "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_extent"
        },
        {
          "type": "bin",
          "field": "distance",
          "as": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
          "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_bins",
          "extent": {
            "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_extent"
          },
          "maxbins": 20
        }
      ]
    },
    {
      "name": "data_0",
      "source": "source_0",
      "transform": [
        {
          "type": "filter",
          "expr": "!length(data(\"brush_store\")) || vlSelectionTest(\"brush_store\", datum, \"union\")"
        }
      ]
    },
    {
      "name": "data_1",
      "source": "data_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_delay\"]) && isFinite(+datum[\"bin_maxbins_20_delay\"])"
        }
      ]
    },
    {
      "name": "data_2",
      "source": "data_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_distance\"]) && isFinite(+datum[\"bin_maxbins_20_distance\"])"
        }
      ]
    },
    {
      "name": "data_3",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_distance\"]) && isFinite(+datum[\"bin_maxbins_20_distance\"])"
        }
      ]
    },
    {
      "name": "data_4",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_delay\"]) && isFinite(+datum[\"bin_maxbins_20_delay\"])"
        }
      ]
    }
  ],
  "signals": [
    {"name": "childWidth", "value": 200},
    {"name": "childHeight", "value": 200},
    {
      "name": "unit",
      "value": {},
      "on": [
        {"events": "mousemove", "update": "isTuple(group()) ? group() : unit"}
      ]
    },
    {
      "name": "brush",
      "update": "vlSelectionResolve(\"brush_store\", \"union\")"
    }
  ],
  "layout": {"padding": 20, "columns": 2, "bounds": "full", "align": "all"},
  "marks": [
    {
      "type": "group",
      "name": "child__column_distance_group",
      "style": "cell",
      "encode": {
        "update": {
          "width": {"signal": "childWidth"},
          "height": {"signal": "childHeight"}
        }
      },
      "signals": [
        {
          "name": "brush_x",
          "value": [],
          "on": [
            {
              "events": {
                "source": "scope",
                "type": "mousedown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"brush_brush\""
                ]
              },
              "update": "[x(unit), x(unit)]"
            },
            {
              "events": {
                "source": "window",
                "type": "mousemove",
                "consume": true,
                "between": [
                  {
                    "source": "scope",
                    "type": "mousedown",
                    "filter": [
                      "!event.item || event.item.mark.name !== \"brush_brush\""
                    ]
                  },
                  {"source": "window", "type": "mouseup"}
                ]
              },
              "update": "[brush_x[0], clamp(x(unit), 0, childWidth)]"
            },
            {
              "events": {"signal": "brush_scale_trigger"},
              "update": "[scale(\"child__column_distance_x\", brush_distance[0]), scale(\"child__column_distance_x\", brush_distance[1])]"
            },
            {
              "events": [{"source": "view", "type": "dblclick"}],
              "update": "[0, 0]"
            },
            {
              "events": {"signal": "brush_translate_delta"},
              "update": "clampRange(panLinear(brush_translate_anchor.extent_x, brush_translate_delta.x / span(brush_translate_anchor.extent_x)), 0, childWidth)"
            },
            {
              "events": {"signal": "brush_zoom_delta"},
              "update": "clampRange(zoomLinear(brush_x, brush_zoom_anchor.x, brush_zoom_delta), 0, childWidth)"
            }
          ]
        },
        {
          "name": "brush_distance",
          "on": [
            {
              "events": {"signal": "brush_x"},
              "update": "brush_x[0] === brush_x[1] ? null : invert(\"child__column_distance_x\", brush_x)"
            }
          ]
        },
        {
          "name": "brush_scale_trigger",
          "value": {},
          "on": [
            {
              "events": [{"scale": "child__column_distance_x"}],
              "update": "(!isArray(brush_distance) || (+invert(\"child__column_distance_x\", brush_x)[0] === +brush_distance[0] && +invert(\"child__column_distance_x\", brush_x)[1] === +brush_distance[1])) ? brush_scale_trigger : {}"
            }
          ]
        },
        {
          "name": "brush_tuple",
          "on": [
            {
              "events": [{"signal": "brush_distance"}],
              "update": "brush_distance ? {unit: \"child__column_distance_layer_0\", fields: brush_tuple_fields, values: [brush_distance]} : null"
            }
          ]
        },
        {
          "name": "brush_tuple_fields",
          "value": [{"field": "distance", "channel": "x", "type": "R"}]
        },
        {
          "name": "brush_translate_anchor",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "mousedown",
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit), extent_x: slice(brush_x)}"
            }
          ]
        },
        {
          "name": "brush_translate_delta",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "window",
                  "type": "mousemove",
                  "consume": true,
                  "between": [
                    {
                      "source": "scope",
                      "type": "mousedown",
                      "markname": "brush_brush"
                    },
                    {"source": "window", "type": "mouseup"}
                  ]
                }
              ],
              "update": "{x: brush_translate_anchor.x - x(unit), y: brush_translate_anchor.y - y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_anchor",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_delta",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "force": true,
              "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
            }
          ]
        },
        {
          "name": "brush_modify",
          "on": [
            {
              "events": {"signal": "brush_tuple"},
              "update": "modify(\"brush_store\", brush_tuple, {unit: \"child__column_distance_layer_0\"})"
            }
          ]
        }
      ],
      "marks": [
        {
          "name": "brush_brush_bg",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {
              "fill": {"value": "#333"},
              "fillOpacity": {"value": 0.125}
            },
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}}
            }
          }
        },
        {
          "name": "child__column_distance_layer_0_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": true,
          "from": {"data": "data_3"},
          "encode": {
            "update": {
              "fill": {"value": "#ddd"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"distance (binned): \" + (!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"]) ? \"null\" : format(datum[\"bin_maxbins_20_distance\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_distance_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance_end"
                }
              ],
              "y": {"scale": "child__column_distance_y", "field": "__count"},
              "y2": {"scale": "child__column_distance_y", "value": 0}
            }
          }
        },
        {
          "name": "child__column_distance_layer_1_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": false,
          "from": {"data": "data_2"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"distance (binned): \" + (!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"]) ? \"null\" : format(datum[\"bin_maxbins_20_distance\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_distance_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance_end"
                }
              ],
              "y": {"scale": "child__column_distance_y", "field": "__count"},
              "y2": {"scale": "child__column_distance_y", "value": 0}
            }
          }
        },
        {
          "name": "brush_brush",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {"fill": {"value": "transparent"}},
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}},
              "stroke": [
                {"test": "brush_x[0] !== brush_x[1]", "value": "white"},
                {"value": null}
              ]
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "child__column_distance_y",
          "orient": "left",
          "gridScale": "child__column_distance_x",
          "grid": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "domain": false,
          "labels": false,
          "aria": false,
          "maxExtent": 0,
          "minExtent": 0,
          "ticks": false,
          "zindex": 0
        },
        {
          "scale": "child__column_distance_x",
          "orient": "bottom",
          "grid": false,
          "title": "distance (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childWidth/10)"},
          "zindex": 0
        },
        {
          "scale": "child__column_distance_y",
          "orient": "left",
          "grid": false,
          "title": "Count of Records",
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "zindex": 0
        }
      ]
    },
    {
      "type": "group",
      "name": "child__column_delay_group",
      "style": "cell",
      "encode": {
        "update": {
          "width": {"signal": "childWidth"},
          "height": {"signal": "childHeight"}
        }
      },
      "signals": [
        {
          "name": "brush_x",
          "value": [],
          "on": [
            {
              "events": {
                "source": "scope",
                "type": "mousedown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"brush_brush\""
                ]
              },
              "update": "[x(unit), x(unit)]"
            },
            {
              "events": {
                "source": "window",
                "type": "mousemove",
                "consume": true,
                "between": [
                  {
                    "source": "scope",
                    "type": "mousedown",
                    "filter": [
                      "!event.item || event.item.mark.name !== \"brush_brush\""
                    ]
                  },
                  {"source": "window", "type": "mouseup"}
                ]
              },
              "update": "[brush_x[0], clamp(x(unit), 0, childWidth)]"
            },
            {
              "events": {"signal": "brush_scale_trigger"},
              "update": "[scale(\"child__column_delay_x\", brush_delay[0]), scale(\"child__column_delay_x\", brush_delay[1])]"
            },
            {
              "events": [{"source": "view", "type": "dblclick"}],
              "update": "[0, 0]"
            },
            {
              "events": {"signal": "brush_translate_delta"},
              "update": "clampRange(panLinear(brush_translate_anchor.extent_x, brush_translate_delta.x / span(brush_translate_anchor.extent_x)), 0, childWidth)"
            },
            {
              "events": {"signal": "brush_zoom_delta"},
              "update": "clampRange(zoomLinear(brush_x, brush_zoom_anchor.x, brush_zoom_delta), 0, childWidth)"
            }
          ]
        },
        {
          "name": "brush_delay",
          "on": [
            {
              "events": {"signal": "brush_x"},
              "update": "brush_x[0] === brush_x[1] ? null : invert(\"child__column_delay_x\", brush_x)"
            }
          ]
        },
        {
          "name": "brush_scale_trigger",
          "value": {},
          "on": [
            {
              "events": [{"scale": "child__column_delay_x"}],
              "update": "(!isArray(brush_delay) || (+invert(\"child__column_delay_x\", brush_x)[0] === +brush_delay[0] && +invert(\"child__column_delay_x\", brush_x)[1] === +brush_delay[1])) ? brush_scale_trigger : {}"
            }
          ]
        },
        {
          "name": "brush_tuple",
          "on": [
            {
              "events": [{"signal": "brush_delay"}],
              "update": "brush_delay ? {unit: \"child__column_delay_layer_0\", fields: brush_tuple_fields, values: [brush_delay]} : null"
            }
          ]
        },
        {
          "name": "brush_tuple_fields",
          "value": [{"field": "delay", "channel": "x", "type": "R"}]
        },
        {
          "name": "brush_translate_anchor",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "mousedown",
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit), extent_x: slice(brush_x)}"
            }
          ]
        },
        {
          "name": "brush_translate_delta",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "window",
                  "type": "mousemove",
                  "consume": true,
                  "between": [
                    {
                      "source": "scope",
                      "type": "mousedown",
                      "markname": "brush_brush"
                    },
                    {"source": "window", "type": "mouseup"}
                  ]
                }
              ],
              "update": "{x: brush_translate_anchor.x - x(unit), y: brush_translate_anchor.y - y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_anchor",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_delta",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "force": true,
              "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
            }
          ]
        },
        {
          "name": "brush_modify",
          "on": [
            {
              "events": {"signal": "brush_tuple"},
              "update": "modify(\"brush_store\", brush_tuple, {unit: \"child__column_delay_layer_0\"})"
            }
          ]
        }
      ],
      "marks": [
        {
          "name": "brush_brush_bg",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {
              "fill": {"value": "#333"},
              "fillOpacity": {"value": 0.125}
            },
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}}
            }
          }
        },
        {
          "name": "child__column_delay_layer_0_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": true,
          "from": {"data": "data_4"},
          "encode": {
            "update": {
              "fill": {"value": "#ddd"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"delay (binned): \" + (!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"]) ? \"null\" : format(datum[\"bin_maxbins_20_delay\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_delay_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay_end"
                }
              ],
              "y": {"scale": "child__column_delay_y", "field": "__count"},
              "y2": {"scale": "child__column_delay_y", "value": 0}
            }
          }
        },
        {
          "name": "child__column_delay_layer_1_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": false,
          "from": {"data": "data_1"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"delay (binned): \" + (!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"]) ? \"null\" : format(datum[\"bin_maxbins_20_delay\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_delay_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay_end"
                }
              ],
              "y": {"scale": "child__column_delay_y", "field": "__count"},
              "y2": {"scale": "child__column_delay_y", "value": 0}
            }
          }
        },
        {
          "name": "brush_brush",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {"fill": {"value": "transparent"}},
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}},
              "stroke": [
                {"test": "brush_x[0] !== brush_x[1]", "value": "white"},
                {"value": null}
              ]
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "child__column_delay_y",
          "orient": "left",
          "gridScale": "child__column_delay_x",
          "grid": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "domain": false,
          "labels": false,
          "aria": false,
          "maxExtent": 0,
          "minExtent": 0,
          "ticks": false,
          "zindex": 0
        },
        {
          "scale": "child__column_delay_x",
          "orient": "bottom",
          "grid": false,
          "title": "delay (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childWidth/10)"},
          "zindex": 0
        },
        {
          "scale": "child__column_delay_y",
          "orient": "left",
          "grid": false,
          "title": "Count of Records",
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "zindex": 0
        }
      ]
    }
  ],
  "scales": [
    {
      "name": "child__column_distance_x",
      "type": "linear",
      "domain": {
        "signal": "[child__column_distance_layer_0_bin_maxbins_20_distance_bins.start, child__column_distance_layer_0_bin_maxbins_20_distance_bins.stop]"
      },
      "range": [0, {"signal": "childWidth"}],
      "bins": {
        "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_bins"
      },
      "zero": false
    },
    {
      "name": "child__column_distance_y",
      "type": "linear",
      "domain": {
        "fields": [
          {"data": "data_3", "field": "__count"},
          {"data": "data_2", "field": "__count"}
        ]
      },
      "range": [{"signal": "childHeight"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "child__column_delay_x",
      "type": "linear",
      "domain": {
        "signal": "[child__column_delay_layer_1_bin_maxbins_20_delay_bins.start, child__column_delay_layer_1_bin_maxbins_20_delay_bins.stop]"
      },
      "range": [0, {"signal": "childWidth"}],
      "bins": {
        "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_bins"
      },
      "zero": false
    },
    {
      "name": "child__column_delay_y",
      "type": "linear",
      "domain": {
        "fields": [
          {"data": "data_4", "field": "__count"},
          {"data": "data_1", "field": "__count"}
        ]
      },
      "range": [{"signal": "childHeight"}, 0],
      "nice": true,
      "zero": true
    }
  ]
}
"##).unwrap()
}

// Data and scale nested into first group
fn spec2() -> ChartSpec {
    serde_json::from_str(r##"{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "data": [
    {"name": "brush_store"},
    {
      "name": "source_0",
      "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/flights-2k.json",
      "format": {"type": "json", "parse": {"date": "date"}},
      "transform": [
        {
          "type": "extent",
          "field": "delay",
          "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_extent"
        },
        {
          "type": "bin",
          "field": "delay",
          "as": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_bins",
          "extent": {
            "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_extent"
          },
          "maxbins": 20
        },
        {
          "type": "extent",
          "field": "distance",
          "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_extent"
        },
        {
          "type": "bin",
          "field": "distance",
          "as": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
          "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_bins",
          "extent": {
            "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_extent"
          },
          "maxbins": 20
        }
      ]
    },
    {
      "name": "data_0",
      "source": "source_0",
      "transform": [
        {
          "type": "filter",
          "expr": "!length(data(\"brush_store\")) || vlSelectionTest(\"brush_store\", datum, \"union\")"
        }
      ]
    },
    {
      "name": "data_1",
      "source": "data_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_delay\"]) && isFinite(+datum[\"bin_maxbins_20_delay\"])"
        }
      ]
    },
    {
      "name": "data_4",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_20_delay", "bin_maxbins_20_delay_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_20_delay\"]) && isFinite(+datum[\"bin_maxbins_20_delay\"])"
        }
      ]
    }
  ],
  "signals": [
    {"name": "childWidth", "value": 200},
    {"name": "childHeight", "value": 200},
    {
      "name": "unit",
      "value": {},
      "on": [
        {"events": "mousemove", "update": "isTuple(group()) ? group() : unit"}
      ]
    },
    {
      "name": "brush",
      "update": "vlSelectionResolve(\"brush_store\", \"union\")"
    }
  ],
  "layout": {"padding": 20, "columns": 2, "bounds": "full", "align": "all"},
  "marks": [
    {
      "type": "group",
      "name": "child__column_distance_group",
      "style": "cell",
      "data": [
        {
          "name": "data_2",
          "source": "data_0",
          "transform": [
            {
              "type": "aggregate",
              "groupby": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
              "ops": ["count"],
              "fields": [null],
              "as": ["__count"]
            },
            {
              "type": "filter",
              "expr": "isValid(datum[\"bin_maxbins_20_distance\"]) && isFinite(+datum[\"bin_maxbins_20_distance\"])"
            }
          ]
        },
        {
          "name": "data_3",
          "source": "source_0",
          "transform": [
            {
              "type": "aggregate",
              "groupby": ["bin_maxbins_20_distance", "bin_maxbins_20_distance_end"],
              "ops": ["count"],
              "fields": [null],
              "as": ["__count"]
            },
            {
              "type": "filter",
              "expr": "isValid(datum[\"bin_maxbins_20_distance\"]) && isFinite(+datum[\"bin_maxbins_20_distance\"])"
            }
          ]
        }
      ],
      "scales": [
        {
          "name": "child__column_distance_x",
          "type": "linear",
          "domain": {
            "signal": "[child__column_distance_layer_0_bin_maxbins_20_distance_bins.start, child__column_distance_layer_0_bin_maxbins_20_distance_bins.stop]"
          },
          "range": [0, {"signal": "childWidth"}],
          "bins": {
            "signal": "child__column_distance_layer_0_bin_maxbins_20_distance_bins"
          },
          "zero": false
        },
        {
          "name": "child__column_distance_y",
          "type": "linear",
          "domain": {
            "fields": [
              {"data": "data_3", "field": "__count"},
              {"data": "data_2", "field": "__count"}
            ]
          },
          "range": [{"signal": "childHeight"}, 0],
          "nice": true,
          "zero": true
        }
      ],
      "encode": {
        "update": {
          "width": {"signal": "childWidth"},
          "height": {"signal": "childHeight"}
        }
      },
      "signals": [
        {
          "name": "brush_x",
          "value": [],
          "on": [
            {
              "events": {
                "source": "scope",
                "type": "mousedown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"brush_brush\""
                ]
              },
              "update": "[x(unit), x(unit)]"
            },
            {
              "events": {
                "source": "window",
                "type": "mousemove",
                "consume": true,
                "between": [
                  {
                    "source": "scope",
                    "type": "mousedown",
                    "filter": [
                      "!event.item || event.item.mark.name !== \"brush_brush\""
                    ]
                  },
                  {"source": "window", "type": "mouseup"}
                ]
              },
              "update": "[brush_x[0], clamp(x(unit), 0, childWidth)]"
            },
            {
              "events": {"signal": "brush_scale_trigger"},
              "update": "[scale(\"child__column_distance_x\", brush_distance[0]), scale(\"child__column_distance_x\", brush_distance[1])]"
            },
            {
              "events": [{"source": "view", "type": "dblclick"}],
              "update": "[0, 0]"
            },
            {
              "events": {"signal": "brush_translate_delta"},
              "update": "clampRange(panLinear(brush_translate_anchor.extent_x, brush_translate_delta.x / span(brush_translate_anchor.extent_x)), 0, childWidth)"
            },
            {
              "events": {"signal": "brush_zoom_delta"},
              "update": "clampRange(zoomLinear(brush_x, brush_zoom_anchor.x, brush_zoom_delta), 0, childWidth)"
            }
          ]
        },
        {
          "name": "brush_distance",
          "on": [
            {
              "events": {"signal": "brush_x"},
              "update": "brush_x[0] === brush_x[1] ? null : invert(\"child__column_distance_x\", brush_x)"
            }
          ]
        },
        {
          "name": "brush_scale_trigger",
          "value": {},
          "on": [
            {
              "events": [{"scale": "child__column_distance_x"}],
              "update": "(!isArray(brush_distance) || (+invert(\"child__column_distance_x\", brush_x)[0] === +brush_distance[0] && +invert(\"child__column_distance_x\", brush_x)[1] === +brush_distance[1])) ? brush_scale_trigger : {}"
            }
          ]
        },
        {
          "name": "brush_tuple",
          "on": [
            {
              "events": [{"signal": "brush_distance"}],
              "update": "brush_distance ? {unit: \"child__column_distance_layer_0\", fields: brush_tuple_fields, values: [brush_distance]} : null"
            }
          ]
        },
        {
          "name": "brush_tuple_fields",
          "value": [{"field": "distance", "channel": "x", "type": "R"}]
        },
        {
          "name": "brush_translate_anchor",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "mousedown",
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit), extent_x: slice(brush_x)}"
            }
          ]
        },
        {
          "name": "brush_translate_delta",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "window",
                  "type": "mousemove",
                  "consume": true,
                  "between": [
                    {
                      "source": "scope",
                      "type": "mousedown",
                      "markname": "brush_brush"
                    },
                    {"source": "window", "type": "mouseup"}
                  ]
                }
              ],
              "update": "{x: brush_translate_anchor.x - x(unit), y: brush_translate_anchor.y - y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_anchor",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_delta",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "force": true,
              "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
            }
          ]
        },
        {
          "name": "brush_modify",
          "on": [
            {
              "events": {"signal": "brush_tuple"},
              "update": "modify(\"brush_store\", brush_tuple, {unit: \"child__column_distance_layer_0\"})"
            }
          ]
        }
      ],
      "marks": [
        {
          "name": "brush_brush_bg",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {
              "fill": {"value": "#333"},
              "fillOpacity": {"value": 0.125}
            },
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}}
            }
          }
        },
        {
          "name": "child__column_distance_layer_0_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": true,
          "from": {"data": "data_3"},
          "encode": {
            "update": {
              "fill": {"value": "#ddd"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"distance (binned): \" + (!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"]) ? \"null\" : format(datum[\"bin_maxbins_20_distance\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_distance_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance_end"
                }
              ],
              "y": {"scale": "child__column_distance_y", "field": "__count"},
              "y2": {"scale": "child__column_distance_y", "value": 0}
            }
          }
        },
        {
          "name": "child__column_distance_layer_1_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": false,
          "from": {"data": "data_2"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"distance (binned): \" + (!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"]) ? \"null\" : format(datum[\"bin_maxbins_20_distance\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_distance_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_distance\"]) || !isFinite(+datum[\"bin_maxbins_20_distance\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_distance_x",
                  "field": "bin_maxbins_20_distance_end"
                }
              ],
              "y": {"scale": "child__column_distance_y", "field": "__count"},
              "y2": {"scale": "child__column_distance_y", "value": 0}
            }
          }
        },
        {
          "name": "brush_brush",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {"fill": {"value": "transparent"}},
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}},
              "stroke": [
                {"test": "brush_x[0] !== brush_x[1]", "value": "white"},
                {"value": null}
              ]
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "child__column_distance_y",
          "orient": "left",
          "gridScale": "child__column_distance_x",
          "grid": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "domain": false,
          "labels": false,
          "aria": false,
          "maxExtent": 0,
          "minExtent": 0,
          "ticks": false,
          "zindex": 0
        },
        {
          "scale": "child__column_distance_x",
          "orient": "bottom",
          "grid": false,
          "title": "distance (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childWidth/10)"},
          "zindex": 0
        },
        {
          "scale": "child__column_distance_y",
          "orient": "left",
          "grid": false,
          "title": "Count of Records",
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "zindex": 0
        }
      ]
    },
    {
      "type": "group",
      "name": "child__column_delay_group",
      "style": "cell",
      "encode": {
        "update": {
          "width": {"signal": "childWidth"},
          "height": {"signal": "childHeight"}
        }
      },
      "signals": [
        {
          "name": "brush_x",
          "value": [],
          "on": [
            {
              "events": {
                "source": "scope",
                "type": "mousedown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"brush_brush\""
                ]
              },
              "update": "[x(unit), x(unit)]"
            },
            {
              "events": {
                "source": "window",
                "type": "mousemove",
                "consume": true,
                "between": [
                  {
                    "source": "scope",
                    "type": "mousedown",
                    "filter": [
                      "!event.item || event.item.mark.name !== \"brush_brush\""
                    ]
                  },
                  {"source": "window", "type": "mouseup"}
                ]
              },
              "update": "[brush_x[0], clamp(x(unit), 0, childWidth)]"
            },
            {
              "events": {"signal": "brush_scale_trigger"},
              "update": "[scale(\"child__column_delay_x\", brush_delay[0]), scale(\"child__column_delay_x\", brush_delay[1])]"
            },
            {
              "events": [{"source": "view", "type": "dblclick"}],
              "update": "[0, 0]"
            },
            {
              "events": {"signal": "brush_translate_delta"},
              "update": "clampRange(panLinear(brush_translate_anchor.extent_x, brush_translate_delta.x / span(brush_translate_anchor.extent_x)), 0, childWidth)"
            },
            {
              "events": {"signal": "brush_zoom_delta"},
              "update": "clampRange(zoomLinear(brush_x, brush_zoom_anchor.x, brush_zoom_delta), 0, childWidth)"
            }
          ]
        },
        {
          "name": "brush_delay",
          "on": [
            {
              "events": {"signal": "brush_x"},
              "update": "brush_x[0] === brush_x[1] ? null : invert(\"child__column_delay_x\", brush_x)"
            }
          ]
        },
        {
          "name": "brush_scale_trigger",
          "value": {},
          "on": [
            {
              "events": [{"scale": "child__column_delay_x"}],
              "update": "(!isArray(brush_delay) || (+invert(\"child__column_delay_x\", brush_x)[0] === +brush_delay[0] && +invert(\"child__column_delay_x\", brush_x)[1] === +brush_delay[1])) ? brush_scale_trigger : {}"
            }
          ]
        },
        {
          "name": "brush_tuple",
          "on": [
            {
              "events": [{"signal": "brush_delay"}],
              "update": "brush_delay ? {unit: \"child__column_delay_layer_0\", fields: brush_tuple_fields, values: [brush_delay]} : null"
            }
          ]
        },
        {
          "name": "brush_tuple_fields",
          "value": [{"field": "delay", "channel": "x", "type": "R"}]
        },
        {
          "name": "brush_translate_anchor",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "mousedown",
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit), extent_x: slice(brush_x)}"
            }
          ]
        },
        {
          "name": "brush_translate_delta",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "window",
                  "type": "mousemove",
                  "consume": true,
                  "between": [
                    {
                      "source": "scope",
                      "type": "mousedown",
                      "markname": "brush_brush"
                    },
                    {"source": "window", "type": "mouseup"}
                  ]
                }
              ],
              "update": "{x: brush_translate_anchor.x - x(unit), y: brush_translate_anchor.y - y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_anchor",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit)}"
            }
          ]
        },
        {
          "name": "brush_zoom_delta",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "brush_brush"
                }
              ],
              "force": true,
              "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
            }
          ]
        },
        {
          "name": "brush_modify",
          "on": [
            {
              "events": {"signal": "brush_tuple"},
              "update": "modify(\"brush_store\", brush_tuple, {unit: \"child__column_delay_layer_0\"})"
            }
          ]
        }
      ],
      "marks": [
        {
          "name": "brush_brush_bg",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {
              "fill": {"value": "#333"},
              "fillOpacity": {"value": 0.125}
            },
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}}
            }
          }
        },
        {
          "name": "child__column_delay_layer_0_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": true,
          "from": {"data": "data_4"},
          "encode": {
            "update": {
              "fill": {"value": "#ddd"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"delay (binned): \" + (!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"]) ? \"null\" : format(datum[\"bin_maxbins_20_delay\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_delay_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay_end"
                }
              ],
              "y": {"scale": "child__column_delay_y", "field": "__count"},
              "y2": {"scale": "child__column_delay_y", "value": 0}
            }
          }
        },
        {
          "name": "child__column_delay_layer_1_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": false,
          "from": {"data": "data_1"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"delay (binned): \" + (!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"]) ? \"null\" : format(datum[\"bin_maxbins_20_delay\"], \"\") + \" – \" + format(datum[\"bin_maxbins_20_delay_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay",
                  "offset": 1
                }
              ],
              "x": [
                {
                  "test": "!isValid(datum[\"bin_maxbins_20_delay\"]) || !isFinite(+datum[\"bin_maxbins_20_delay\"])",
                  "value": 0
                },
                {
                  "scale": "child__column_delay_x",
                  "field": "bin_maxbins_20_delay_end"
                }
              ],
              "y": {"scale": "child__column_delay_y", "field": "__count"},
              "y2": {"scale": "child__column_delay_y", "value": 0}
            }
          }
        },
        {
          "name": "brush_brush",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {"fill": {"value": "transparent"}},
            "update": {
              "x": {"signal": "brush_x[0]"},
              "y": {"value": 0},
              "x2": {"signal": "brush_x[1]"},
              "y2": {"field": {"group": "height"}},
              "stroke": [
                {"test": "brush_x[0] !== brush_x[1]", "value": "white"},
                {"value": null}
              ]
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "child__column_delay_y",
          "orient": "left",
          "gridScale": "child__column_delay_x",
          "grid": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "domain": false,
          "labels": false,
          "aria": false,
          "maxExtent": 0,
          "minExtent": 0,
          "ticks": false,
          "zindex": 0
        },
        {
          "scale": "child__column_delay_x",
          "orient": "bottom",
          "grid": false,
          "title": "delay (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childWidth/10)"},
          "zindex": 0
        },
        {
          "scale": "child__column_delay_y",
          "orient": "left",
          "grid": false,
          "title": "Count of Records",
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(childHeight/40)"},
          "zindex": 0
        }
      ]
    }
  ],
  "scales": [
    {
      "name": "child__column_delay_x",
      "type": "linear",
      "domain": {
        "signal": "[child__column_delay_layer_1_bin_maxbins_20_delay_bins.start, child__column_delay_layer_1_bin_maxbins_20_delay_bins.stop]"
      },
      "range": [0, {"signal": "childWidth"}],
      "bins": {
        "signal": "child__column_delay_layer_1_bin_maxbins_20_delay_bins"
      },
      "zero": false
    },
    {
      "name": "child__column_delay_y",
      "type": "linear",
      "domain": {
        "fields": [
          {"data": "data_4", "field": "__count"},
          {"data": "data_1", "field": "__count"}
        ]
      },
      "range": [{"signal": "childHeight"}, 0],
      "nice": true,
      "zero": true
    }
  ]
}"##).unwrap()
}

fn weather_spec() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "width": 20,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "source_0",
      "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/seattle-weather.csv",
      "format": {"type": "csv", "delimiter": ","},
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["weather"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "stack",
          "groupby": [],
          "field": "__count",
          "sort": {"field": ["weather"], "order": ["descending"]},
          "as": ["__count_start", "__count_end"],
          "offset": "zero"
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
          "fill": {"scale": "color", "field": "weather"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"Count of Records: \" + (format(datum[\"__count\"], \"\")) + \"; Weather type: \" + (isValid(datum[\"weather\"]) ? datum[\"weather\"] : \"\"+datum[\"weather\"])"
          },
          "xc": {"signal": "width", "mult": 0.5},
          "width": {"value": 18},
          "y": {"scale": "y", "field": "__count_end"},
          "y2": {"scale": "y", "field": "__count_start"}
        }
      }
    }
  ],
  "scales": [
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
    "##).unwrap()
}