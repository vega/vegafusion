use crate::error::Result;
use crate::planning::plan::{PlannerConfig, SpecPlan};
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::values::StringOrSignalSpec;
use itertools::Itertools;
use json_patch::{diff, patch, PatchOperation};
use serde_json::Value;

/// Attempt to apply the difference between two vega specs to a third pre-transformed spec
pub fn patch_pre_transformed_spec(
    spec1: &ChartSpec,
    pre_transformed_spec1: &ChartSpec,
    spec2: &ChartSpec,
) -> Result<Option<ChartSpec>> {
    // Run spec1 and spec2 through the client portion of the pre_transform_spec logic.
    // This performs domain splitting and introduces projection pushdown transforms.
    let planner_config = PlannerConfig {
        extract_server_data: false,
        ..Default::default()
    };
    let plan1 = SpecPlan::try_new(spec1, &planner_config)?;
    let planned_spec1 = plan1.client_spec;

    let plan2 = SpecPlan::try_new(spec2, &planner_config)?;
    let planned_spec2 = plan2.client_spec;

    // Diff the planned specs to create patch between them
    let spec_patch = diff(
        &serde_json::to_value(planned_spec1)?,
        &serde_json::to_value(planned_spec2)?,
    );

    // Do not apply patch if there are changes to any datasets
    for patch_op in &spec_patch.0 {
        let path = match patch_op {
            PatchOperation::Add(op) => &op.path,
            PatchOperation::Remove(op) => &op.path,
            PatchOperation::Replace(op) => &op.path,
            PatchOperation::Move(op) => &op.path,
            PatchOperation::Copy(op) => &op.path,
            PatchOperation::Test(op) => &op.path,
        };
        if path.contains("/data/") {
            return Ok(None);
        }
    }

    // Attempt to apply patch to pre-transformed spec
    let mut pre_transformed_spec2 = serde_json::to_value(pre_transformed_spec1)?;
    if patch(&mut pre_transformed_spec2, spec_patch.0.as_slice()).is_err() {
        // Patch failed to apply, return None
        Ok(None)
    } else {
        // Convert objects with index keys (like {"0": "foo", "1": "bar"})
        // to arrays (like ["foo", "bar"])
        let pre_transformed_spec2 = arrayify_int_key_objects(&pre_transformed_spec2);

        // Patch applied successfully, check validity
        let pre_transformed_spec2: ChartSpec = serde_json::from_value(pre_transformed_spec2)?;

        // Check for presence of inline dataset URLs, this indicates an invalid pre transformed spec
        let mut visitor = AnyInlineDatasetUrlsVisitor::new();
        pre_transformed_spec2.walk(&mut visitor)?;
        if visitor.any_inline_dataset_urls {
            return Ok(None);
        }

        Ok(Some(pre_transformed_spec2))
    }
}

/// When replacing an object with an array, the patched chart will sometimes end up with
/// objects of the form {"0": "A", "1": "BB"}. This function identifies such objects and converts
/// them into arrays (["A", "BB"]).
fn arrayify_int_key_objects(obj: &Value) -> Value {
    match obj {
        Value::Object(map) => {
            if !map.is_empty() && map.keys().all(|k| k.parse::<usize>().is_ok()) {
                // Object is not empty and all keys unsigned integer strings
                let indices: Vec<_> = map
                    .keys()
                    .map(|k| k.parse::<usize>().unwrap())
                    .sorted()
                    .collect();
                let max_index = indices[indices.len() - 1];
                let mut new_array = vec![Value::Null; max_index + 1];
                for idx in indices {
                    new_array[idx] = arrayify_int_key_objects(&map[&idx.to_string()]);
                }
                Value::Array(new_array)
            } else {
                // Recurse into the sub-object otherwise
                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), arrayify_int_key_objects(v));
                }
                Value::Object(new_map)
            }
        }
        Value::Array(arr) => Value::Array(arr.iter().map(arrayify_int_key_objects).collect()),
        _ => obj.clone(),
    }
}

struct AnyInlineDatasetUrlsVisitor {
    pub any_inline_dataset_urls: bool,
}

impl AnyInlineDatasetUrlsVisitor {
    pub fn new() -> Self {
        Self {
            any_inline_dataset_urls: false,
        }
    }
}

impl ChartVisitor for AnyInlineDatasetUrlsVisitor {
    fn visit_data(&mut self, data: &DataSpec, _scope: &[u32]) -> Result<()> {
        if let Some(StringOrSignalSpec::String(url)) = &data.url {
            if url.starts_with("table://") || url.starts_with("vegafusion+dataset://") {
                self.any_inline_dataset_urls = true;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::patch::patch_pre_transformed_spec;
    use crate::spec::chart::ChartSpec;
    use serde_json::{json, Value};

    fn histogram(fill: Value, max_bins: u32, add_dataset: bool) -> ChartSpec {
        let mut chart_spec: ChartSpec = serde_json::from_value(json!(
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
                      "maxbins": max_bins
                    },
                    {
                      "type": "aggregate",
                      "groupby": [
                        "bin_maxbins_10_IMDB Rating",
                        "bin_maxbins_10_IMDB Rating_end"
                      ],
                      "ops": ["count"],
                      "fields": [null],
                      "as": ["__count"]
                    },
                    {
                      "type": "filter",
                      "expr": "isValid(datum[\"bin_maxbins_10_IMDB Rating\"]) && isFinite(+datum[\"bin_maxbins_10_IMDB Rating\"])"
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
                      "fill": fill,
                      "ariaRoleDescription": {"value": "bar"},
                      "x2": {
                        "scale": "x",
                        "field": "bin_maxbins_10_IMDB Rating",
                        "offset": 1
                      },
                      "x": {"scale": "x", "field": "bin_maxbins_10_IMDB Rating_end"},
                      "y": {"scale": "y", "field": "__count"},
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
                  "domain": {"data": "source_0", "field": "__count"},
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
                  "title": "Count of Records",
                  "labelOverlap": true,
                  "tickCount": {"signal": "ceil(height/40)"},
                  "zindex": 0
                }
              ]
            }
        )).unwrap();

        if add_dataset {
            chart_spec.data.push(
                serde_json::from_value(json!(
                    {
                        "name": "added_dataset",
                        "values": [{"a": 1, "b": 2}]
                    }
                ))
                .unwrap(),
            )
        }

        chart_spec
    }

    fn pre_transformed_histogram(fill: Value) -> ChartSpec {
        serde_json::from_value(json!(
            {
              "$schema": "https://vega.github.io/schema/vega/v5.json",
              "data": [
                {
                  "name": "source_0",
                  "values": [
                    {
                      "__count": 985,
                      "bin_maxbins_10_IMDB Rating": 6.0,
                      "bin_maxbins_10_IMDB Rating_end": 7.0
                    },
                    {
                      "__count": 100,
                      "bin_maxbins_10_IMDB Rating": 3.0,
                      "bin_maxbins_10_IMDB Rating_end": 4.0
                    },
                    {
                      "__count": 741,
                      "bin_maxbins_10_IMDB Rating": 7.0,
                      "bin_maxbins_10_IMDB Rating_end": 8.0
                    },
                    {
                      "__count": 633,
                      "bin_maxbins_10_IMDB Rating": 5.0,
                      "bin_maxbins_10_IMDB Rating_end": 6.0
                    },
                    {
                      "__count": 204,
                      "bin_maxbins_10_IMDB Rating": 8.0,
                      "bin_maxbins_10_IMDB Rating_end": 9.0
                    },
                    {
                      "__count": 43,
                      "bin_maxbins_10_IMDB Rating": 2.0,
                      "bin_maxbins_10_IMDB Rating_end": 3.0
                    },
                    {
                      "__count": 273,
                      "bin_maxbins_10_IMDB Rating": 4.0,
                      "bin_maxbins_10_IMDB Rating_end": 5.0
                    },
                    {
                      "__count": 4,
                      "bin_maxbins_10_IMDB Rating": 9.0,
                      "bin_maxbins_10_IMDB Rating_end": 10.0
                    },
                    {
                      "__count": 5,
                      "bin_maxbins_10_IMDB Rating": 1.0,
                      "bin_maxbins_10_IMDB Rating_end": 2.0
                    }
                  ]
                },
                {
                  "name": "source_0_y_domain___count",
                  "values": [
                    {
                      "min": 4,
                      "max": 985
                    }
                  ]
                }
              ],
              "signals": [
                {
                  "name": "bin_maxbins_10_IMDB_Rating_bins",
                  "value": {
                    "fields": [
                      "IMDB Rating"
                    ],
                    "fname": "bin_IMDB Rating",
                    "start": 1.0,
                    "step": 1.0,
                    "stop": 10.0
                  }
                }
              ],
              "marks": [
                {
                  "type": "rect",
                  "name": "marks",
                  "from": {
                    "data": "source_0"
                  },
                  "encode": {
                    "update": {
                      "x": {
                        "field": "bin_maxbins_10_IMDB Rating_end",
                        "scale": "x"
                      },
                      "ariaRoleDescription": {
                        "value": "bar"
                      },
                      "y": {
                        "field": "__count",
                        "scale": "y"
                      },
                      "y2": {
                        "value": 0,
                        "scale": "y"
                      },
                      "fill": fill,
                      "x2": {
                        "field": "bin_maxbins_10_IMDB Rating",
                        "scale": "x",
                        "offset": 1
                      }
                    }
                  },
                  "style": [
                    "bar"
                  ]
                }
              ],
              "scales": [
                {
                  "name": "x",
                  "type": "linear",
                  "domain": {
                    "signal": "[bin_maxbins_10_IMDB_Rating_bins.start, bin_maxbins_10_IMDB_Rating_bins.stop]"
                  },
                  "range": [
                    0,
                    {
                      "signal": "width"
                    }
                  ],
                  "bins": {
                    "signal": "bin_maxbins_10_IMDB_Rating_bins"
                  },
                  "zero": false
                },
                {
                  "name": "y",
                  "type": "linear",
                  "domain": [
                    {
                      "signal": "(data(\"source_0_y_domain___count\")[0] || {}).min"
                    },
                    {
                      "signal": "(data(\"source_0_y_domain___count\")[0] || {}).max"
                    }
                  ],
                  "range": [
                    {
                      "signal": "height"
                    },
                    0
                  ],
                  "zero": true,
                  "nice": true
                }
              ],
              "axes": [
                {
                  "scale": "y",
                  "grid": true,
                  "gridScale": "x",
                  "labels": false,
                  "minExtent": 0,
                  "tickCount": {
                    "signal": "ceil(height/40)"
                  },
                  "maxExtent": 0,
                  "domain": false,
                  "zindex": 0,
                  "aria": false,
                  "ticks": false,
                  "orient": "left"
                },
                {
                  "scale": "x",
                  "orient": "bottom",
                  "labelOverlap": true,
                  "grid": false,
                  "title": "IMDB Rating (binned)",
                  "labelFlush": true,
                  "tickCount": {
                    "signal": "ceil(width/10)"
                  },
                  "zindex": 0
                },
                {
                  "scale": "y",
                  "zindex": 0,
                  "grid": false,
                  "labelOverlap": true,
                  "tickCount": {
                    "signal": "ceil(height/40)"
                  },
                  "orient": "left",
                  "title": "Count of Records"
                }
              ],
              "height": 200,
              "style": "cell",
              "background": "white",
              "padding": 5,
              "width": 200
            }
        )).unwrap()
    }

    #[test]
    fn test_patch_color_succeeds() {
        let spec1: ChartSpec = histogram(json!({"value": "blue"}), 10, false);

        let spec2: ChartSpec = histogram(json!({"value": "red"}), 10, false);

        let pre_transformed_spec1: ChartSpec = pre_transformed_histogram(json!({"value": "blue"}));

        let pre_transform_spec2 =
            patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
                .expect("Expected patch_pre_transformed_spec to succeed")
                .expect("Expected patch_pre_transformed_spec to return Some");

        assert_eq!(
            pre_transform_spec2,
            pre_transformed_histogram(json!({"value": "red"}))
        )
    }

    #[test]
    fn test_patch_color_array_succeeds() {
        // Replace an object with an array of objects
        let fill_value = json!({"value": "blue"});
        let fill_array = json!([{"test": "2 < 3", "value": "red"}, {"value": "green"}]);

        let spec1: ChartSpec = histogram(fill_value.clone(), 10, false);
        let spec2: ChartSpec = histogram(fill_array.clone(), 10, false);
        let pre_transformed_spec1: ChartSpec = pre_transformed_histogram(fill_value);

        let pre_transform_spec2 =
            patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
                .expect("Expected patch_pre_transformed_spec to succeed")
                .expect("Expected patch_pre_transformed_spec to return Some");

        let encode_spec = pre_transform_spec2.marks[0].encode.clone().unwrap();
        let update_spec = encode_spec.encodings.get("update").unwrap();
        let fill = update_spec.channels.get("fill").unwrap();
        let fill_str = serde_json::to_string(fill).unwrap();
        assert_eq!(
            fill_str,
            r#"[{"value":"red","test":"2 < 3"},{"value":"green"}]"#
        );
    }

    #[test]
    fn test_patch_max_bins_fails() {
        let spec1: ChartSpec = histogram(json!({"value": "blue"}), 10, false);

        let spec2: ChartSpec = histogram(json!({"value": "blue"}), 20, false);

        let pre_transformed_spec1: ChartSpec = pre_transformed_histogram(json!({"value": "blue"}));

        let pre_transform_spec2 =
            patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
                .expect("Expected patch_pre_transformed_spec to succeed");
        assert!(pre_transform_spec2.is_none());
    }

    #[test]
    fn test_patch_adds_inline_dataset() {
        let spec1: ChartSpec = serde_json::from_value(json!(
            {
                "data": [
                    {
                        "name": "data1",
                        "url": "something.csv"
                    }
                ]
            }
        ))
        .unwrap();

        let spec2: ChartSpec = serde_json::from_value(json!(
            {
                "data": [
                    {
                        "name": "data1",
                        "url": "table://something"
                    }
                ]
            }
        ))
        .unwrap();

        let pre_transformed_spec1 = spec1.clone();

        let pre_transform_spec2 =
            patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
                .expect("Expected patch_pre_transformed_spec to succeed");

        assert!(pre_transform_spec2.is_none());
    }

    #[test]
    fn test_patch_add_dataset_fails() {
        let spec1: ChartSpec = histogram(json!({"value": "blue"}), 10, false);

        let spec2: ChartSpec = histogram(json!({"value": "red"}), 10, true);

        let pre_transformed_spec1: ChartSpec = pre_transformed_histogram(json!({"value": "blue"}));

        let pre_transform_spec2 =
            patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
                .expect("Expected patch_pre_transformed_spec to succeed");
        assert!(pre_transform_spec2.is_none());
    }
}
