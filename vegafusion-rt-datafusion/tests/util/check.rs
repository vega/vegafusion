use crate::util::equality::{
    assert_signals_almost_equal, assert_tables_equal, normalize_scalar, TablesEqualConfig,
};
use crate::util::vegajs_runtime::vegajs_runtime;
use datafusion::scalar::ScalarValue;
use std::str::FromStr;

use std::convert::TryFrom;
use vegafusion_core::data::scalar::ScalarValueHelpers;

use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;
use vegafusion_core::expression::parser::parse;
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
use vegafusion_rt_datafusion::expression::compiler::compile;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;
use vegafusion_rt_datafusion::expression::compiler::utils::ExprHelpers;
use vegafusion_rt_datafusion::task_graph::timezone::RuntimeTzConfig;
use vegafusion_rt_datafusion::tokio_runtime::TOKIO_RUNTIME;
use vegafusion_rt_datafusion::transform::pipeline::TransformPipelineUtils;

pub fn check_expr_supported(expr_str: &str) {
    let expr = parse(expr_str).unwrap();
    assert!(expr.is_supported())
}

pub fn check_parsing(expr_str: &str) {
    let vegajs_runtime = vegajs_runtime();
    let expected = vegajs_runtime.parse_expression(expr_str).unwrap();
    let mut result = parse(expr_str).unwrap();
    result.clear_spans();

    assert_eq!(result, expected, " left: {}\nright: {}\n", result, expected);
}

pub fn check_scalar_evaluation(expr_str: &str, config: &CompilationConfig) {
    // Use block here to drop vegajs_runtime lock before the potential assert_eq error
    // This avoids poisoning the Mutex if the assertion fails
    let vegajs_runtime = vegajs_runtime();
    let expected = vegajs_runtime
        .eval_scalar_expression(expr_str, config)
        .unwrap();
    let expected = normalize_scalar(&expected);

    // Add local timezone info to config
    let local_tz_str = vegajs_runtime.nodejs_runtime.local_timezone().unwrap();
    // let local_tz = chrono_tz::Tz::from_str(&local_tz_str).unwrap();
    let config = CompilationConfig {
        tz_config: Some(RuntimeTzConfig::try_new(&local_tz_str, &None).unwrap()),
        ..config.clone()
    };

    // Vega-Fusion parse
    let parsed = parse(expr_str).unwrap();

    // Build compilation config
    let compiled = compile(&parsed, &config, None).unwrap();
    let result = compiled.eval_to_scalar().unwrap();

    // Serialize and deserialize to normalize types to those supported by JavaScript
    // (e.g. Int to Float)
    let result = ScalarValue::from_json(&result.to_json().unwrap()).unwrap();
    let result = normalize_scalar(&result);

    println!("{:?}", result);
    assert_eq!(result, expected, " left: {}\nright: {}\n", result, expected);
}

pub fn check_transform_evaluation(
    data: &VegaFusionTable,
    transform_specs: &[TransformSpec],
    compilation_config: &CompilationConfig,
    equality_config: &TablesEqualConfig,
) {
    let vegajs_runtime = vegajs_runtime();
    let local_tz_str = vegajs_runtime.nodejs_runtime.local_timezone().unwrap();
    let local_tz = chrono_tz::Tz::from_str(&local_tz_str).unwrap();
    // Add local timezone info to compilation config
    let compilation_config = CompilationConfig {
        tz_config: Some(RuntimeTzConfig {
            local_tz,
            default_input_tz: local_tz,
        }),
        ..compilation_config.clone()
    };

    let (expected_data, expected_signals) = vegajs_runtime
        .eval_transform(data, transform_specs, &compilation_config)
        .unwrap();

    // println!(
    //     "expected data\n{}",
    //     expected_data.pretty_format(Some(500)).unwrap()
    // );
    // println!("expected signals: {:?}", expected_signals);

    let (result_data, result_signals) =
        eval_vegafusion_transforms(data, transform_specs, &compilation_config);

    // println!(
    //     "result data\n{}",
    //     result_data.pretty_format(Some(500)).unwrap()
    // );
    // println!("result signals: {:?}", result_signals);

    assert_tables_equal(&result_data, &expected_data, equality_config);
    assert_signals_almost_equal(result_signals, expected_signals, equality_config.tolerance);
}

pub fn eval_vegafusion_transforms(
    data: &VegaFusionTable,
    transform_specs: &[TransformSpec],
    compilation_config: &CompilationConfig,
) -> (VegaFusionTable, Vec<ScalarValue>) {
    // add ordering column
    let data = data.clone().with_ordering().unwrap();
    let pipeline = TransformPipeline::try_from(transform_specs).unwrap();
    let sql_df = (*TOKIO_RUNTIME).block_on(data.to_sql_dataframe()).unwrap();

    let (result_data, result_signals) = TOKIO_RUNTIME
        .block_on(pipeline.eval_sql(sql_df, compilation_config))
        .unwrap();
    let result_signals = result_signals
        .into_iter()
        .map(|v| v.as_scalar().map(|v| v.clone()))
        .collect::<Result<Vec<ScalarValue>>>()
        .unwrap();
    (result_data, result_signals)
}
