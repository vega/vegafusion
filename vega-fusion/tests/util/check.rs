use crate::util::equality::{assert_tables_equal, TablesEqualConfig};
use crate::util::vegajs_runtime::vegajs_runtime;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::convert::TryFrom;
use vega_fusion::data::table::VegaFusionTable;
use vega_fusion::expression::compiler::compile;
use vega_fusion::expression::compiler::config::CompilationConfig;
use vega_fusion::expression::compiler::utils::ExprHelpers;
use vega_fusion::expression::parser::parse;
use vega_fusion::spec::transform::TransformSpec;
use vega_fusion::transform::pipeline::TransformPipeline;

pub fn check_parsing(expr_str: &str) {
    let vegajs_runtime = vegajs_runtime();
    let expected = vegajs_runtime.parse_expression(expr_str).unwrap();
    let mut result = parse(expr_str).unwrap();
    result.clear_spans();

    assert_eq!(
        result,
        expected,
        " left: {}\nright: {}\n",
        result.to_string(),
        expected.to_string()
    );
}

pub fn check_scalar_evaluation(expr_str: &str, scope: &HashMap<String, ScalarValue>) {
    // Use block here to drop vegajs_runtime lock before the potential assert_eq error
    // This avoids poisoning the Mutex if the assertion fails
    let vegajs_runtime = vegajs_runtime();
    let expected = vegajs_runtime
        .eval_scalar_expression(expr_str, scope)
        .unwrap();

    // Vega-Fusion parse
    let parsed = parse(expr_str).unwrap();

    // Build compilation config
    let config = CompilationConfig {
        signal_scope: scope.clone(),
        ..Default::default()
    };

    let compiled = compile(&parsed, &config, None).unwrap();
    let result = compiled.eval_to_scalar().unwrap();

    assert_eq!(
        result,
        expected,
        " left: {}\nright: {}\n",
        result.to_string(),
        expected.to_string()
    );
}

pub fn check_transform_evaluation(
    data: &VegaFusionTable,
    transform_specs: &[TransformSpec],
    compilation_config: &CompilationConfig,
    equality_config: &TablesEqualConfig,
) {
    let vegajs_runtime = vegajs_runtime();

    let df = data.to_dataframe().unwrap();
    let pipeline = TransformPipeline::try_from(transform_specs).unwrap();
    let (result_df, result_signals) = pipeline.call(df, compilation_config).unwrap();

    let result_data = VegaFusionTable::try_from(result_df).unwrap();

    let (expected_data, expected_signals) = vegajs_runtime
        .eval_transform(data, transform_specs, compilation_config)
        .unwrap();

    println!(
        "result data\n{}",
        result_data.pretty_format(Some(100)).unwrap()
    );
    println!("result signals: {:?}", result_signals);

    assert_tables_equal(&result_data, &expected_data, equality_config);
    assert_eq!(
        result_signals, expected_signals,
        "Signals do not match\nlhs: {:?}, rhs: {:?}",
        result_signals, expected_signals
    );
}
