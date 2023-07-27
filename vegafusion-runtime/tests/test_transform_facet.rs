#[macro_use]
extern crate lazy_static;

mod util;

use crate::util::check::eval_vegafusion_transforms;
use datafusion_common::ScalarValue;
use serde_json::json;
use util::check::check_transform_evaluation;
use util::datasets::vega_json_dataset;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_runtime::expression::compiler::config::CompilationConfig;

#[test]
fn test_facet_1() {
    let dataset = vega_json_dataset("penguins");

    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "facet",
            "groupby": ["Sex"],
            "transform": [
                {
                    "type": "aggregate",
                    "groupby": [],
                    "fields": ["Island"],
                    "op": ["count"],
                    "as": ["count_beak_depth"]
                }
            ]
        },
    ]))
    .unwrap();

    let (result_data, result_signals) =
        eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &Default::default());

    assert_eq!(
        result_data.pretty_format(None).unwrap(),
        "\
+--------+------------------+
| Sex    | count_beak_depth |
+--------+------------------+
| MALE   | 168              |
| FEMALE | 165              |
|        | 10               |
| .      | 1                |
+--------+------------------+"
    )
}

#[test]
fn test_facet_2() {
    let dataset = vega_json_dataset("penguins");

    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "facet",
            "groupby": ["Sex", "Island"],
            "transform": [
                {
                    "type": "aggregate",
                    "groupby": [],
                    "fields": ["Island"],
                    "op": ["count"],
                    "as": ["count_beak_depth"]
                }
            ]
        },
    ]))
    .unwrap();

    let (result_data, result_signals) =
        eval_vegafusion_transforms(&dataset, transform_specs.as_slice(), &Default::default());

    assert_eq!(
        result_data.pretty_format(None).unwrap(),
        "\
+--------+-----------+------------------+
| Sex    | Island    | count_beak_depth |
+--------+-----------+------------------+
| MALE   | Torgersen | 23               |
| FEMALE | Torgersen | 24               |
|        | Torgersen | 5                |
| FEMALE | Biscoe    | 80               |
| MALE   | Biscoe    | 83               |
| FEMALE | Dream     | 61               |
| MALE   | Dream     | 62               |
|        | Dream     | 1                |
|        | Biscoe    | 4                |
| .      | Biscoe    | 1                |
+--------+-----------+------------------+"
    )
}
