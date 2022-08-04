/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use vegafusion_core::planning::extract::extract_server_data;
use vegafusion_core::proto::gen::tasks::{TaskGraph, TzConfig, Variable};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

use std::collections::HashSet;
use std::sync::Arc;
use vegafusion_core::planning::plan::PlannerWarnings;
use vegafusion_core::planning::split_domain_data::split_domain_data;

use vegafusion_core::planning::stitch::stitch_specs;

#[tokio::test(flavor = "multi_thread")]
async fn test_extract_server_data() {
    let mut spec = spec1();
    let tz_config = TzConfig {
        local_tz: "America/New_York".to_string(),
        default_input_tz: None,
    };

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();
    // println!("{:#?}", task_scope);

    let mut warnings: Vec<PlannerWarnings> = Vec::new();
    let server_spec = extract_server_data(
        &mut spec,
        &mut task_scope,
        &mut warnings,
        &Default::default(),
    )
    .unwrap();
    // println!("{}", serde_json::to_string_pretty(&server_spec).unwrap());

    let client_defs: HashSet<_> = spec.definition_vars().unwrap().into_iter().collect();
    let client_inputs: HashSet<_> = spec.input_vars(&task_scope).unwrap().into_iter().collect();
    let client_updates: HashSet<_> = spec.update_vars(&task_scope).unwrap().into_iter().collect();

    let server_defs: HashSet<_> = server_spec.definition_vars().unwrap().into_iter().collect();
    let server_inputs: HashSet<_> = server_spec
        .input_vars(&task_scope)
        .unwrap()
        .into_iter()
        .collect();
    let server_updates: HashSet<_> = server_spec
        .update_vars(&task_scope)
        .unwrap()
        .into_iter()
        .collect();

    let client_to_server: Vec<_> = client_updates.intersection(&server_inputs).collect();
    println!("client_to_server: {:?}", client_to_server);

    let server_to_client: Vec<_> = server_updates.intersection(&client_inputs).collect();
    println!("server_to_client: {:?}", server_to_client);

    let server_stubs: Vec<_> = server_inputs.difference(&server_defs).collect();
    println!("server_stubs: {:?}", server_stubs);

    let client_stubs: Vec<_> = client_inputs.difference(&client_defs).collect();
    println!("client_stubs: {:?}", client_stubs);

    let tasks = server_spec
        .to_tasks(&tz_config, &Default::default())
        .unwrap();
    let graph = Arc::new(TaskGraph::new(tasks, &task_scope).unwrap());
    let mapping = graph.build_mapping();
    // println!("{:#?}", mapping);

    let graph_runtime = TaskGraphRuntime::new(Some(20), Some(1024_i32.pow(3) as usize));
    let _data_3 = graph_runtime
        .get_node_value(
            graph.clone(),
            mapping
                .get(&(Variable::new_data("data_3"), Vec::new()))
                .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();

    // println!("data_3:\n{}", data_3.into_table().unwrap().pretty_format(None).unwrap());

    let _delay_extent = graph_runtime
        .get_node_value(
            graph.clone(),
            mapping
                .get(&(
                    Variable::new_signal("child__column_delay_layer_1_bin_maxbins_20_delay_extent"),
                    Vec::new(),
                ))
                .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();

    // println!("delay_extent: {:?}", delay_extent.into_scalar().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_extract_stitch_data() {
    let mut spec = spec1();

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();

    let mut warnings: Vec<PlannerWarnings> = Vec::new();
    let mut server_spec = extract_server_data(
        &mut spec,
        &mut task_scope,
        &mut warnings,
        &Default::default(),
    )
    .unwrap();
    let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut spec).unwrap();

    println!("{:#?}", comm_plan);

    println!(
        "client spec:\n{}",
        serde_json::to_string_pretty(&spec).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn try_extract_split_server_data() {
    let mut spec = weather_spec();

    // Get full spec's scope
    let mut task_scope = spec.to_task_scope().unwrap();

    let mut warnings: Vec<PlannerWarnings> = Vec::new();
    let mut server_spec = extract_server_data(
        &mut spec,
        &mut task_scope,
        &mut warnings,
        &Default::default(),
    )
    .unwrap();
    let comm_plan = stitch_specs(&task_scope, &mut server_spec, &mut spec).unwrap();

    println!("{:#?}", comm_plan);

    println!(
        "server spec:\n{}\n\n",
        serde_json::to_string_pretty(&server_spec).unwrap()
    );
    println!(
        "client spec:\n{}\n\n",
        serde_json::to_string_pretty(&spec).unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn try_split_domain() {
    // let mut spec = sorted_bar_spec();
    let mut spec = cars_scatter_spec();
    split_domain_data(&mut spec).unwrap();
    println!("{}", serde_json::to_string_pretty(&spec).unwrap())
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
fn histogram_responsive() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "background": "white",
  "padding": 5,
  "width": 600,
  "data": [
    {"name": "selector001_store"},
    {
      "name": "source_0",
      "url": "https://cdn.jsdelivr.net/npm/vega-datasets@v1.29.0/data/flights-5k.json",
      "format": {"type": "json"},
      "transform": [
        {
          "type": "formula",
          "expr": "hours(datum.date) + minutes(datum.date) / 60",
          "as": "time"
        },
        {
          "type": "extent",
          "field": "time",
          "signal": "concat_1_bin_maxbins_30_time_extent"
        },
        {
          "type": "bin",
          "field": "time",
          "as": ["bin_maxbins_30_time", "bin_maxbins_30_time_end"],
          "signal": "concat_1_bin_maxbins_30_time_bins",
          "extent": {"signal": "concat_1_bin_maxbins_30_time_extent"},
          "maxbins": 30
        },
        {
          "type": "extent",
          "field": "time",
          "signal": "concat_0_bin_extent_param_selector001_maxbins_30_time_extent"
        },
        {
          "type": "bin",
          "field": "time",
          "as": [
            "bin_extent_param_selector001_maxbins_30_time",
            "bin_extent_param_selector001_maxbins_30_time_end"
          ],
          "signal": "concat_0_bin_extent_param_selector001_maxbins_30_time_bins",
          "extent": {
            "signal": "concat_0_bin_extent_param_selector001_maxbins_30_time_extent"
          },
          "span": {"signal": "span(selector001[\"time\"])"},
          "maxbins": 30
        }
      ]
    },
    {
      "name": "data_0",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["bin_maxbins_30_time", "bin_maxbins_30_time_end"],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_maxbins_30_time\"]) && isFinite(+datum[\"bin_maxbins_30_time\"])"
        }
      ]
    },
    {
      "name": "data_1",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": [
            "bin_extent_param_selector001_maxbins_30_time",
            "bin_extent_param_selector001_maxbins_30_time_end"
          ],
          "ops": ["count"],
          "fields": [null],
          "as": ["__count"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"bin_extent_param_selector001_maxbins_30_time\"]) && isFinite(+datum[\"bin_extent_param_selector001_maxbins_30_time\"])"
        }
      ]
    }
  ],
  "signals": [
    {"name": "childHeight", "value": 100},
    {
      "name": "unit",
      "value": {},
      "on": [
        {"events": "mousemove", "update": "isTuple(group()) ? group() : unit"}
      ]
    },
    {
      "name": "selector001",
      "update": "vlSelectionResolve(\"selector001_store\", \"union\")"
    }
  ],
  "layout": {"padding": 20, "columns": 1, "bounds": "full", "align": "each"},
  "marks": [
    {
      "type": "group",
      "name": "concat_0_group",
      "style": "cell",
      "encode": {
        "update": {
          "width": {"signal": "width"},
          "height": {"signal": "childHeight"}
        }
      },
      "marks": [
        {
          "name": "concat_0_marks",
          "type": "rect",
          "clip": true,
          "style": ["bar"],
          "interactive": false,
          "from": {"data": "data_1"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"time (binned): \" + (!isValid(datum[\"bin_extent_param_selector001_maxbins_30_time\"]) || !isFinite(+datum[\"bin_extent_param_selector001_maxbins_30_time\"]) ? \"null\" : format(datum[\"bin_extent_param_selector001_maxbins_30_time\"], \"\") + \" – \" + format(datum[\"bin_extent_param_selector001_maxbins_30_time_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": {
                "scale": "concat_0_x",
                "field": "bin_extent_param_selector001_maxbins_30_time",
                "offset": 1
              },
              "x": {
                "scale": "concat_0_x",
                "field": "bin_extent_param_selector001_maxbins_30_time_end"
              },
              "y": {"scale": "concat_0_y", "field": "__count"},
              "y2": {"scale": "concat_0_y", "value": 0}
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "concat_0_y",
          "orient": "left",
          "gridScale": "concat_0_x",
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
          "scale": "concat_0_x",
          "orient": "bottom",
          "grid": false,
          "title": "time (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(width/10)"},
          "zindex": 0
        },
        {
          "scale": "concat_0_y",
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
      "name": "concat_1_group",
      "style": "cell",
      "encode": {
        "update": {
          "width": {"signal": "width"},
          "height": {"signal": "childHeight"}
        }
      },
      "signals": [
        {
          "name": "selector001_x",
          "value": [],
          "on": [
            {
              "events": {
                "source": "scope",
                "type": "mousedown",
                "filter": [
                  "!event.item || event.item.mark.name !== \"selector001_brush\""
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
                      "!event.item || event.item.mark.name !== \"selector001_brush\""
                    ]
                  },
                  {"source": "window", "type": "mouseup"}
                ]
              },
              "update": "[selector001_x[0], clamp(x(unit), 0, width)]"
            },
            {
              "events": {"signal": "selector001_scale_trigger"},
              "update": "[scale(\"concat_1_x\", selector001_time[0]), scale(\"concat_1_x\", selector001_time[1])]"
            },
            {
              "events": [{"source": "view", "type": "dblclick"}],
              "update": "[0, 0]"
            },
            {
              "events": {"signal": "selector001_translate_delta"},
              "update": "clampRange(panLinear(selector001_translate_anchor.extent_x, selector001_translate_delta.x / span(selector001_translate_anchor.extent_x)), 0, width)"
            },
            {
              "events": {"signal": "selector001_zoom_delta"},
              "update": "clampRange(zoomLinear(selector001_x, selector001_zoom_anchor.x, selector001_zoom_delta), 0, width)"
            }
          ]
        },
        {
          "name": "selector001_time",
          "on": [
            {
              "events": {"signal": "selector001_x"},
              "update": "selector001_x[0] === selector001_x[1] ? null : invert(\"concat_1_x\", selector001_x)"
            }
          ]
        },
        {
          "name": "selector001_scale_trigger",
          "value": {},
          "on": [
            {
              "events": [{"scale": "concat_1_x"}],
              "update": "(!isArray(selector001_time) || (+invert(\"concat_1_x\", selector001_x)[0] === +selector001_time[0] && +invert(\"concat_1_x\", selector001_x)[1] === +selector001_time[1])) ? selector001_scale_trigger : {}"
            }
          ]
        },
        {
          "name": "selector001_tuple",
          "on": [
            {
              "events": [{"signal": "selector001_time"}],
              "update": "selector001_time ? {unit: \"concat_1\", fields: selector001_tuple_fields, values: [selector001_time]} : null"
            }
          ]
        },
        {
          "name": "selector001_tuple_fields",
          "value": [{"field": "time", "channel": "x", "type": "R"}]
        },
        {
          "name": "selector001_translate_anchor",
          "value": {},
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "mousedown",
                  "markname": "selector001_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit), extent_x: slice(selector001_x)}"
            }
          ]
        },
        {
          "name": "selector001_translate_delta",
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
                      "markname": "selector001_brush"
                    },
                    {"source": "window", "type": "mouseup"}
                  ]
                }
              ],
              "update": "{x: selector001_translate_anchor.x - x(unit), y: selector001_translate_anchor.y - y(unit)}"
            }
          ]
        },
        {
          "name": "selector001_zoom_anchor",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "selector001_brush"
                }
              ],
              "update": "{x: x(unit), y: y(unit)}"
            }
          ]
        },
        {
          "name": "selector001_zoom_delta",
          "on": [
            {
              "events": [
                {
                  "source": "scope",
                  "type": "wheel",
                  "consume": true,
                  "markname": "selector001_brush"
                }
              ],
              "force": true,
              "update": "pow(1.001, event.deltaY * pow(16, event.deltaMode))"
            }
          ]
        },
        {
          "name": "selector001_modify",
          "on": [
            {
              "events": {"signal": "selector001_tuple"},
              "update": "modify(\"selector001_store\", selector001_tuple, true)"
            }
          ]
        }
      ],
      "marks": [
        {
          "name": "selector001_brush_bg",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {
              "fill": {"value": "#333"},
              "fillOpacity": {"value": 0.125}
            },
            "update": {
              "x": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "signal": "selector001_x[0]"
                },
                {"value": 0}
              ],
              "y": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "value": 0
                },
                {"value": 0}
              ],
              "x2": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "signal": "selector001_x[1]"
                },
                {"value": 0}
              ],
              "y2": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "field": {"group": "height"}
                },
                {"value": 0}
              ]
            }
          }
        },
        {
          "name": "concat_1_marks",
          "type": "rect",
          "style": ["bar"],
          "interactive": true,
          "from": {"data": "data_0"},
          "encode": {
            "update": {
              "fill": {"value": "#4c78a8"},
              "ariaRoleDescription": {"value": "bar"},
              "description": {
                "signal": "\"time (binned): \" + (!isValid(datum[\"bin_maxbins_30_time\"]) || !isFinite(+datum[\"bin_maxbins_30_time\"]) ? \"null\" : format(datum[\"bin_maxbins_30_time\"], \"\") + \" – \" + format(datum[\"bin_maxbins_30_time_end\"], \"\")) + \"; Count of Records: \" + (format(datum[\"__count\"], \"\"))"
              },
              "x2": {
                "scale": "concat_1_x",
                "field": "bin_maxbins_30_time",
                "offset": 1
              },
              "x": {"scale": "concat_1_x", "field": "bin_maxbins_30_time_end"},
              "y": {"scale": "concat_1_y", "field": "__count"},
              "y2": {"scale": "concat_1_y", "value": 0}
            }
          }
        },
        {
          "name": "selector001_brush",
          "type": "rect",
          "clip": true,
          "encode": {
            "enter": {"fill": {"value": "transparent"}},
            "update": {
              "x": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "signal": "selector001_x[0]"
                },
                {"value": 0}
              ],
              "y": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "value": 0
                },
                {"value": 0}
              ],
              "x2": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "signal": "selector001_x[1]"
                },
                {"value": 0}
              ],
              "y2": [
                {
                  "test": "data(\"selector001_store\").length && data(\"selector001_store\")[0].unit === \"concat_1\"",
                  "field": {"group": "height"}
                },
                {"value": 0}
              ],
              "stroke": [
                {
                  "test": "selector001_x[0] !== selector001_x[1]",
                  "value": "white"
                },
                {"value": null}
              ]
            }
          }
        }
      ],
      "axes": [
        {
          "scale": "concat_1_y",
          "orient": "left",
          "gridScale": "concat_1_x",
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
          "scale": "concat_1_x",
          "orient": "bottom",
          "grid": false,
          "title": "time (binned)",
          "labelFlush": true,
          "labelOverlap": true,
          "tickCount": {"signal": "ceil(width/10)"},
          "zindex": 0
        },
        {
          "scale": "concat_1_y",
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
      "name": "concat_0_x",
      "type": "linear",
      "domain": {
        "signal": "[concat_0_bin_extent_param_selector001_maxbins_30_time_bins.start, concat_0_bin_extent_param_selector001_maxbins_30_time_bins.stop]"
      },
      "domainRaw": {"signal": "selector001[\"time\"]"},
      "range": [0, {"signal": "width"}],
      "bins": {
        "signal": "concat_0_bin_extent_param_selector001_maxbins_30_time_bins"
      },
      "zero": false
    },
    {
      "name": "concat_0_y",
      "type": "linear",
      "domain": {"data": "data_1", "field": "__count"},
      "range": [{"signal": "childHeight"}, 0],
      "nice": true,
      "zero": true
    },
    {
      "name": "concat_1_x",
      "type": "linear",
      "domain": {
        "signal": "[concat_1_bin_maxbins_30_time_bins.start, concat_1_bin_maxbins_30_time_bins.stop]"
      },
      "range": [0, {"signal": "width"}],
      "bins": {"signal": "concat_1_bin_maxbins_30_time_bins"},
      "zero": false
    },
    {
      "name": "concat_1_y",
      "type": "linear",
      "domain": {"data": "data_0", "field": "__count"},
      "range": [{"signal": "childHeight"}, 0],
      "nice": true,
      "zero": true
    }
  ]
}
    "##).unwrap()
}

// Data and scale nested into first group
#[allow(dead_code)]
fn sorted_bar_spec() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "description": "A bar chart that sorts the y-values by the x-values.",
  "background": "white",
  "padding": 5,
  "width": 200,
  "style": "cell",
  "data": [
    {
      "name": "source_0",
      "url": "data/population.json",
      "format": {"type": "json"},
      "transform": [{"type": "filter", "expr": "datum.year == 2000"}]
    },
    {
      "name": "data_0",
      "source": "source_0",
      "transform": [
        {
          "type": "aggregate",
          "groupby": ["age"],
          "ops": ["sum"],
          "fields": ["people"],
          "as": ["sum_people"]
        },
        {
          "type": "filter",
          "expr": "isValid(datum[\"sum_people\"]) && isFinite(+datum[\"sum_people\"])"
        }
      ]
    }
  ],
  "signals": [
    {"name": "y_step", "value": 17},
    {
      "name": "height",
      "update": "bandspace(domain('y').length, 0.1, 0.05) * y_step"
    }
  ],
  "marks": [
    {
      "name": "marks",
      "type": "rect",
      "style": ["bar"],
      "from": {"data": "data_0"},
      "encode": {
        "update": {
          "fill": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "bar"},
          "description": {
            "signal": "\"population: \" + (format(datum[\"sum_people\"], \"\")) + \"; age: \" + (isValid(datum[\"age\"]) ? datum[\"age\"] : \"\"+datum[\"age\"])"
          },
          "x": {"scale": "x", "field": "sum_people"},
          "x2": {"scale": "x", "value": 0},
          "y": {"scale": "y", "field": "age"},
          "height": {"scale": "y", "band": 1}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "linear",
      "domain": {"data": "data_0", "field": "sum_people"},
      "range": [0, {"signal": "width"}],
      "nice": true,
      "zero": true
    },
    {
      "name": "y",
      "type": "band",
      "domain": {
        "data": "source_0",
        "field": "age",
        "sort": {"op": "sum", "field": "people", "order": "descending"}
      },
      "range": {"step": {"signal": "y_step"}},
      "paddingInner": 0.1,
      "paddingOuter": 0.05
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
      "scale": "x",
      "orient": "bottom",
      "grid": false,
      "title": "population",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "zindex": 0
    },
    {"scale": "y", "orient": "left", "grid": false, "title": "age", "zindex": 0}
  ]
}
    "##).unwrap()
}

// Data and scale nested into first group
#[allow(dead_code)]
fn cars_scatter_spec() -> ChartSpec {
    serde_json::from_str(r##"
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "description": "A scatterplot showing horsepower and miles per gallons for various cars.",
  "background": "white",
  "padding": 5,
  "width": 200,
  "height": 200,
  "style": "cell",
  "data": [
    {
      "name": "source_0",
      "url": "data/cars.json",
      "format": {"type": "json"},
      "transform": [
        {
          "type": "filter",
          "expr": "isValid(datum[\"Horsepower\"]) && isFinite(+datum[\"Horsepower\"]) && isValid(datum[\"Miles_per_Gallon\"]) && isFinite(+datum[\"Miles_per_Gallon\"])"
        }
      ]
    }
  ],
  "marks": [
    {
      "name": "marks",
      "type": "symbol",
      "style": ["point"],
      "from": {"data": "source_0"},
      "encode": {
        "update": {
          "opacity": {"value": 0.7},
          "fill": {"value": "transparent"},
          "stroke": {"value": "#4c78a8"},
          "ariaRoleDescription": {"value": "point"},
          "description": {
            "signal": "\"Horsepower: \" + (format(datum[\"Horsepower\"], \"\")) + \"; Miles_per_Gallon: \" + (format(datum[\"Miles_per_Gallon\"], \"\"))"
          },
          "x": {"scale": "x", "field": "Horsepower"},
          "y": {"scale": "y", "field": "Miles_per_Gallon"}
        }
      }
    }
  ],
  "scales": [
    {
      "name": "x",
      "type": "linear",
      "domain": {"data": "source_0", "field": "Horsepower"},
      "range": [0, {"signal": "width"}],
      "nice": true,
      "zero": true
    },
    {
      "name": "y",
      "type": "linear",
      "domain": {"data": "source_0", "field": "Miles_per_Gallon"},
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
      "title": "Horsepower",
      "labelFlush": true,
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(width/40)"},
      "zindex": 0
    },
    {
      "scale": "y",
      "orient": "left",
      "grid": false,
      "title": "Miles_per_Gallon",
      "labelOverlap": true,
      "tickCount": {"signal": "ceil(height/40)"},
      "zindex": 0
    }
  ]
}
    "##).unwrap()
}
