#[macro_use]
extern crate lazy_static;

mod util;

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use util::check::{check_transform_evaluation, eval_vegafusion_transforms};
use util::equality::{assert_tables_equal, TablesEqualConfig};
use vegafusion_common::arrow::array::{Float64Array, StringArray};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::spec::scale::ScaleTypeSpec;
use vegafusion_core::spec::transform::formula::FormulaTransformSpec;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_core::task_graph::scale_state::ScaleState;
use vegafusion_runtime::data::util::SessionContextUtils;
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use vegafusion_runtime::expression::compiler::config::CompilationConfig;
use vegafusion_runtime::tokio_runtime::TOKIO_RUNTIME;
use vegafusion_runtime::transform::pipeline::TransformPipelineUtils;

fn base_dataset() -> VegaFusionTable {
    VegaFusionTable::from_json(&json!([
        {"v": 2.0, "m": 1.0},
        {"v": 7.0, "m": 0.0}
    ]))
    .unwrap()
}

fn scale_config() -> CompilationConfig {
    let mut config = CompilationConfig::default();
    config.scale_scope.insert(
        "x".to_string(),
        ScaleState {
            scale_type: ScaleTypeSpec::Linear,
            domain: Arc::new(Float64Array::from(vec![0.0, 10.0])),
            range: Arc::new(Float64Array::from(vec![0.0, 100.0])),
            options: HashMap::new(),
        },
    );
    config
}

fn heatmap_scale_config() -> CompilationConfig {
    let mut config = CompilationConfig::default();
    config.scale_scope.insert(
        "color".to_string(),
        ScaleState {
            scale_type: ScaleTypeSpec::Linear,
            domain: Arc::new(Float64Array::from(vec![76.0, 158.0])),
            range: Arc::new(StringArray::from(vec![
                "#eff9bd", "#dbf1b4", "#bde5b5", "#94d5b9", "#69c5be", "#45b4c2", "#2c9ec0",
                "#2182b8", "#2163aa", "#23479c", "#1c3185",
            ])),
            options: HashMap::new(),
        },
    );
    config
}

#[test]
fn test_mark_encoding_scale_conditional_offset_matches_formula_equivalent() {
    let dataset = base_dataset();

    let mark_encoding_tx: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "mark_encoding",
            "encode_set": "update",
            "channels": [
                {
                    "channel": "x",
                    "as": "x_px",
                    "encoding": {"field": "v", "scale": "x", "offset": 2}
                },
                {
                    "channel": "opacity",
                    "as": "opacity",
                    "encoding": [
                        {"test": "datum.m > 0", "value": 1},
                        {"value": 0.2}
                    ]
                }
            ]
        }
    ]))
    .unwrap();

    let expected_formula_tx = vec![
        TransformSpec::Formula(FormulaTransformSpec {
            expr: "scale('x', datum.v) + 2".to_string(),
            as_: "x_px".to_string(),
            extra: Default::default(),
        }),
        TransformSpec::Formula(FormulaTransformSpec {
            expr: "datum.m > 0 ? 1 : 0.2".to_string(),
            as_: "opacity".to_string(),
            extra: Default::default(),
        }),
    ];

    let config = scale_config();

    // Compare mark_encoding output to a formula-based equivalent under identical runtime scope.
    let (mark_result, _) =
        eval_vegafusion_transforms(&dataset, mark_encoding_tx.as_slice(), &config);
    let (formula_result, _) =
        eval_vegafusion_transforms(&dataset, expected_formula_tx.as_slice(), &config);

    assert_tables_equal(&mark_result, &formula_result, &TablesEqualConfig::default());
}

#[test]
fn test_mark_encoding_missing_scale_returns_null() {
    let dataset = base_dataset();
    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "mark_encoding",
            "channels": [
                {
                    "channel": "x",
                    "as": "x_px",
                    "encoding": {"field": "v", "scale": "missing"}
                }
            ]
        }
    ]))
    .unwrap();

    let (result, _) = eval_vegafusion_transforms(
        &dataset,
        transform_specs.as_slice(),
        &CompilationConfig::default(),
    );
    let rb = result.to_record_batch().unwrap();
    let col_idx = rb.schema().index_of("x_px").unwrap();
    let x_px = rb.column(col_idx);

    assert_eq!(x_px.len(), 2);
    assert!(datafusion_common::ScalarValue::try_from_array(x_px, 0)
        .unwrap()
        .is_null());
    assert!(datafusion_common::ScalarValue::try_from_array(x_px, 1)
        .unwrap()
        .is_null());
}

#[test]
fn test_mark_encoding_conditional_matches_vega_formula_equivalent() {
    let dataset = base_dataset();
    let mark_encoding_tx: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "mark_encoding",
            "channels": [
                {
                    "channel": "opacity",
                    "as": "opacity",
                    "encoding": [
                        {"test": "datum.m > 0", "value": 1},
                        {"value": 0.2}
                    ]
                }
            ]
        }
    ]))
    .unwrap();

    let formula_tx = vec![TransformSpec::Formula(FormulaTransformSpec {
        expr: "datum.m > 0 ? 1 : 0.2".to_string(),
        as_: "opacity".to_string(),
        extra: Default::default(),
    })];

    // Validate equivalent formula expression semantics against Vega.
    check_transform_evaluation(
        &dataset,
        formula_tx.as_slice(),
        &CompilationConfig::default(),
        &TablesEqualConfig::default(),
    );

    // Then ensure mark_encoding produces identical results to the Vega-validated equivalent.
    let (mark_result, _) = eval_vegafusion_transforms(
        &dataset,
        mark_encoding_tx.as_slice(),
        &CompilationConfig::default(),
    );
    let (formula_result, _) = eval_vegafusion_transforms(
        &dataset,
        formula_tx.as_slice(),
        &CompilationConfig::default(),
    );
    assert_tables_equal(&mark_result, &formula_result, &TablesEqualConfig::default());
}

#[test]
fn test_mark_encoding_unsupported_field_object_errors() {
    let dataset = base_dataset().with_ordering().unwrap();
    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "mark_encoding",
            "channels": [
                {
                    "channel": "x",
                    "as": "x_px",
                    "encoding": {"field": {"group": "foo"}}
                }
            ]
        }
    ]))
    .unwrap();

    let pipeline = TransformPipeline::try_from(transform_specs.as_slice()).unwrap();
    let config = CompilationConfig::default();
    let ctx = Arc::new(make_datafusion_context());
    let sql_df = TOKIO_RUNTIME
        .block_on(ctx.vegafusion_table(dataset))
        .unwrap();
    let err = TOKIO_RUNTIME
        .block_on(pipeline.eval_sql(sql_df, &config))
        .unwrap_err();

    assert!(format!("{err}").contains("field objects"));
}

#[test]
fn test_mark_encoding_heatmap_fill_outputs_hex_strings() {
    let dataset = VegaFusionTable::from_json(&json!([
        {"v": 76.0},
        {"v": 100.0},
        {"v": 158.0}
    ]))
    .unwrap();

    let transform_specs: Vec<TransformSpec> = serde_json::from_value(json!([
        {
            "type": "mark_encoding",
            "encode_set": "update",
            "channels": [
                {
                    "channel": "fill",
                    "as": "fill",
                    "encoding": {"field": "v", "scale": "color"}
                }
            ]
        }
    ]))
    .unwrap();

    let (result, _) = eval_vegafusion_transforms(
        &dataset,
        transform_specs.as_slice(),
        &heatmap_scale_config(),
    );
    let rb = result.to_record_batch().unwrap();
    let col_idx = rb.schema().index_of("fill").unwrap();
    let fill = rb.column(col_idx);

    let mut values_debug = Vec::with_capacity(fill.len());
    for i in 0..fill.len() {
        let scalar = datafusion_common::ScalarValue::try_from_array(fill, i).unwrap();
        values_debug.push(format!("{scalar:?}"));
        let hex = match scalar {
            datafusion_common::ScalarValue::Utf8(Some(v))
            | datafusion_common::ScalarValue::LargeUtf8(Some(v))
            | datafusion_common::ScalarValue::Utf8View(Some(v)) => v,
            _ => panic!("Expected string color output, got {scalar:?}"),
        };
        assert!(
            hex.starts_with('#'),
            "Expected hex color string, got {hex:?}"
        );
        assert!(
            matches!(hex.len(), 7 | 9),
            "Expected #RRGGBB or #RRGGBBAA string length, got {} for {hex:?}",
            hex.len()
        );
        for ch in hex.chars().skip(1) {
            assert!(
                ch.is_ascii_hexdigit(),
                "Expected hex digits in color string, got {hex:?}"
            );
        }
    }
    assert!(!values_debug.is_empty());
}
