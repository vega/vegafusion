use crate::expression::compiler::builtin_functions::control_flow::if_fn::if_fn;
use crate::expression::compiler::builtin_functions::date_time::datetime::{
    datetime_transform_fn, make_datetime_components_fn, to_date_transform,
};

#[cfg(feature = "scales")]
use crate::datafusion::udfs::scale::make_scale_udf;
use crate::expression::compiler::builtin_functions::array::indexof::indexof_transform;
use crate::expression::compiler::builtin_functions::array::length::length_transform;
use crate::expression::compiler::builtin_functions::array::span::span_transform;
use crate::expression::compiler::builtin_functions::data::data_fn::data_fn;
use crate::expression::compiler::builtin_functions::data::vl_selection_resolve::vl_selection_resolve_fn;
use crate::expression::compiler::builtin_functions::data::vl_selection_test::vl_selection_test_fn;
use crate::expression::compiler::builtin_functions::date_time::date_format::{
    time_format_fn, utc_format_fn,
};
use crate::expression::compiler::builtin_functions::date_time::date_parts::{
    DATE_TRANSFORM, DAYOFYEAR_TRANSFORM, DAY_TRANSFORM, HOUR_TRANSFORM, MILLISECOND_TRANSFORM,
    MINUTE_TRANSFORM, MONTH_TRANSFORM, QUARTER_TRANSFORM, SECOND_TRANSFORM, UTCDATE_TRANSFORM,
    UTCDAYOFYEAR_TRANSFORM, UTCDAY_TRANSFORM, UTCHOUR_TRANSFORM, UTCMILLISECOND_TRANSFORM,
    UTCMINUTE_TRANSFORM, UTCMONTH_TRANSFORM, UTCQUARTER_TRANSFORM, UTCSECOND_TRANSFORM,
    UTCYEAR_TRANSFORM, YEAR_TRANSFORM,
};
use crate::expression::compiler::builtin_functions::date_time::time::time_fn;
use crate::expression::compiler::builtin_functions::date_time::time_offset::time_offset_fn;
use crate::expression::compiler::builtin_functions::format::format_transform;
use crate::expression::compiler::builtin_functions::math::isfinite::is_finite_fn;
use crate::expression::compiler::builtin_functions::type_checking::isdate::is_date_fn;
use crate::expression::compiler::builtin_functions::type_checking::isvalid::is_valid_fn;
use crate::expression::compiler::builtin_functions::type_coercion::to_boolean::to_boolean_transform;
use crate::expression::compiler::builtin_functions::type_coercion::to_number::to_number_transform;
use crate::expression::compiler::builtin_functions::type_coercion::to_string::to_string_transform;
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::lit;
use datafusion_expr::{expr, Expr, ScalarUDF};
use datafusion_functions::core::least;
use datafusion_functions::expr_fn::isnan;
use datafusion_functions::math::{
    abs, acos, asin, atan, ceil, cos, exp, floor, ln, power, round, sin, sqrt, tan,
};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datafusion_common::ScalarValue;
use vegafusion_common::datatypes::cast_to;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::expression::{
    expression, literal, CallExpression, Expression, Literal,
};

pub type MacroFn = Arc<dyn Fn(&[Expression]) -> Result<Expression> + Send + Sync>;
pub type TransformFn = Arc<dyn Fn(&[Expr], &DFSchema) -> Result<Expr> + Send + Sync>;
pub type ScalarTransformFn = Arc<dyn Fn(Expr) -> Expr + Send + Sync>;
pub type TzTransformFn =
    Arc<dyn Fn(&RuntimeTzConfig, &[Expr], &DFSchema) -> Result<Expr> + Send + Sync>;
pub type DataFn = Arc<
    dyn Fn(&VegaFusionTable, &[Expression], &DFSchema, &RuntimeTzConfig) -> Result<Expr>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub enum VegaFusionCallable {
    /// A function that operates on the ESTree expression tree before compilation
    Macro(MacroFn),

    /// A function that operates on the compiled arguments and produces a new expression.
    Transform(TransformFn),

    /// An infallible function that operates on a single compiled argument
    UnaryTransform(ScalarTransformFn),

    /// A function that uses the local timezone to operate on the compiled arguments and
    /// produces a new expression.
    LocalTransform(TzTransformFn),

    /// A function that uses the UTC timezone to operate on the compiled arguments and
    /// produces a new expression.
    UtcTransform(TzTransformFn),

    /// A custom runtime function that's not built into DataFusion
    ScalarUDF {
        udf: Arc<ScalarUDF>,
        /// If Some, all arguments should be cast to provided type
        cast: Option<DataType>,
    },

    /// A custom macro that inputs a dataset, and uses that to generate the DataFusion Expr tree
    ///
    /// e.g. `data('brush')` or  `vlSelectionTest('brush', datum, true)`
    Data(DataFn),

    /// A custom runtime function that operates on a scale dataset
    Scale { invert: bool },
}

pub fn compile_scalar_arguments(
    node: &CallExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
    cast: &Option<DataType>,
) -> Result<Vec<Expr>> {
    let mut args: Vec<Expr> = Vec::new();
    for arg in &node.arguments {
        let compiled_arg = compile(arg, config, Some(schema))?;
        let arg_expr = match cast {
            None => compiled_arg,
            Some(dtype) => cast_to(compiled_arg, dtype, schema)?,
        };
        args.push(arg_expr);
    }
    Ok(args)
}

pub fn compile_call(
    node: &CallExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let callable = config.callable_scope.get(&node.callee).ok_or_else(|| {
        VegaFusionError::compilation(format!("No global function named {}", &node.callee))
    })?;

    match callable {
        VegaFusionCallable::Macro(callable) => {
            // Apply macro then recursively compile
            let new_expr = callable(&node.arguments)?;
            compile(&new_expr, config, Some(schema))
        }
        VegaFusionCallable::ScalarUDF { udf, cast } => {
            let args = compile_scalar_arguments(node, config, schema, cast)?;
            Ok(Expr::ScalarFunction(expr::ScalarFunction {
                func: udf.clone(),
                args,
            }))
        }
        VegaFusionCallable::Data(callee) => {
            if let Some(v) = node.arguments.first() {
                match v.expr() {
                    expression::Expr::Literal(Literal {
                        value: Some(literal::Value::String(name)),
                        ..
                    }) => {
                        if let Some(dataset) = config.data_scope.get(name) {
                            let tz_config = config
                                .tz_config
                                .with_context(|| "No local timezone info provided".to_string())?;

                            callee(dataset, &node.arguments[1..], schema, &tz_config)
                        } else {
                            Err(VegaFusionError::internal(format!(
                                "No dataset named {}. Available: {:?}",
                                name,
                                config.data_scope.keys()
                            )))
                        }
                    }
                    _ => Err(VegaFusionError::internal(format!(
                        "The first argument to the {} function must be a literal \
                                string with the name of a dataset",
                        &node.callee
                    ))),
                }
            } else {
                Err(VegaFusionError::internal(format!(
                    "The first argument to the {} function must be a literal \
                                string with the name of a dataset",
                    &node.callee
                )))
            }
        }
        VegaFusionCallable::Transform(callable) => {
            let args = compile_scalar_arguments(node, config, schema, &None)?;
            callable(&args, schema)
        }
        VegaFusionCallable::UnaryTransform(callable) => {
            let mut args = compile_scalar_arguments(node, config, schema, &None)?;
            if args.len() != 1 {
                Err(VegaFusionError::internal(format!(
                    "The {} function requires 1 argument. Received {}",
                    &node.callee,
                    args.len()
                )))
            } else {
                Ok(callable(args.pop().unwrap()))
            }
        }
        VegaFusionCallable::LocalTransform(callable) => {
            let args = compile_scalar_arguments(node, config, schema, &None)?;
            let tz_config = config
                .tz_config
                .with_context(|| "No local timezone info provided".to_string())?;
            callable(&tz_config, &args, schema)
        }
        VegaFusionCallable::UtcTransform(callable) => {
            let args = compile_scalar_arguments(node, config, schema, &None)?;
            let tz_config = RuntimeTzConfig {
                local_tz: chrono_tz::UTC,
                default_input_tz: chrono_tz::UTC,
            };
            callable(&tz_config, &args, schema)
        }
        VegaFusionCallable::Scale { invert } => compile_scale_call(node, config, schema, *invert),
    }
}

fn scale_name_arg(node: &CallExpression, fn_name: &str) -> Result<String> {
    let arg0 = node.arguments.first().ok_or_else(|| {
        VegaFusionError::compilation(format!(
            "The {fn_name} function requires a scale name as the first argument"
        ))
    })?;

    match arg0.expr() {
        expression::Expr::Literal(Literal {
            value: Some(literal::Value::String(name)),
            ..
        }) => Ok(name.clone()),
        _ => Err(VegaFusionError::compilation(format!(
            "The first argument to the {fn_name} function must be a literal string scale name"
        ))),
    }
}

fn compile_scale_call(
    node: &CallExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
    invert: bool,
) -> Result<Expr> {
    let fn_name = if invert { "invert" } else { "scale" };
    if node.arguments.len() == 3 {
        return Err(VegaFusionError::compilation(format!(
            "The optional group argument to {fn_name} is not yet supported by the VegaFusion runtime"
        )));
    }
    if node.arguments.len() != 2 {
        return Err(VegaFusionError::compilation(format!(
            "The {fn_name} function requires exactly 2 arguments in this phase. Received {}",
            node.arguments.len()
        )));
    }

    let scale_name = scale_name_arg(node, fn_name)?;
    let value_expr = compile(&node.arguments[1], config, Some(schema))?;

    #[cfg(not(feature = "scales"))]
    {
        let _ = scale_name;
        let _ = value_expr;
        return Err(VegaFusionError::compilation(format!(
            "The {fn_name} function requires the vegafusion-runtime `scales` feature"
        )));
    }

    #[cfg(feature = "scales")]
    let Some(scale_state) = config.scale_scope.get(&scale_name) else {
        // Vega returns undefined for unknown scales. Represent this as SQL NULL.
        return Ok(lit(ScalarValue::Null));
    };

    #[cfg(feature = "scales")]
    {
        let udf = make_scale_udf(&scale_name, invert, scale_state, &config.tz_config)?;
        Ok(Expr::ScalarFunction(expr::ScalarFunction {
            func: udf,
            args: vec![value_expr],
        }))
    }
}

fn min_fn(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    // Vega min delegates to JavaScript Math.min, which returns +Infinity for zero arguments.
    if args.is_empty() {
        return Ok(lit(ScalarValue::Float64(Some(f64::INFINITY))));
    }

    let cast_args = args
        .iter()
        .map(|arg| cast_to(arg.clone(), &DataType::Float64, schema))
        .collect::<Result<Vec<_>>>()?;
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: least(),
        args: cast_args,
    }))
}

pub fn default_callables() -> HashMap<String, VegaFusionCallable> {
    let mut callables: HashMap<String, VegaFusionCallable> = HashMap::new();
    callables.insert("if".to_string(), VegaFusionCallable::Macro(Arc::new(if_fn)));

    callables.insert(
        "scale".to_string(),
        VegaFusionCallable::Scale { invert: false },
    );
    callables.insert(
        "invert".to_string(),
        VegaFusionCallable::Scale { invert: true },
    );

    // Numeric functions built into DataFusion with mapping to Vega names
    for (fun_name, udf) in [
        ("abs", abs()),
        ("acos", acos()),
        ("asin", asin()),
        ("atan", atan()),
        ("ceil", ceil()),
        ("cos", cos()),
        ("exp", exp()),
        ("floor", floor()),
        ("round", round()),
        ("sin", sin()),
        ("sqrt", sqrt()),
        ("tan", tan()),
        ("pow", power()),
        ("log", ln()), // Vega log is DataFusion ln
    ] {
        callables.insert(
            fun_name.to_string(),
            VegaFusionCallable::ScalarUDF {
                udf,
                cast: Some(DataType::Float64),
            },
        );
    }

    callables.insert(
        "min".to_string(),
        VegaFusionCallable::Transform(Arc::new(min_fn)),
    );

    callables.insert(
        "isNaN".to_string(),
        VegaFusionCallable::UnaryTransform(Arc::new(isnan)),
    );

    callables.insert(
        "isFinite".to_string(),
        VegaFusionCallable::Transform(Arc::new(is_finite_fn)),
    );

    callables.insert(
        "isValid".to_string(),
        VegaFusionCallable::Transform(Arc::new(is_valid_fn)),
    );

    callables.insert(
        "isDate".to_string(),
        VegaFusionCallable::Transform(Arc::new(is_date_fn)),
    );

    callables.insert(
        "length".to_string(),
        VegaFusionCallable::Transform(Arc::new(length_transform)),
    );

    callables.insert(
        "span".to_string(),
        VegaFusionCallable::Transform(Arc::new(span_transform)),
    );

    callables.insert(
        "indexof".to_string(),
        VegaFusionCallable::Transform(Arc::new(indexof_transform)),
    );

    // Date parts
    callables.insert(
        "year".to_string(),
        VegaFusionCallable::LocalTransform(YEAR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "quarter".to_string(),
        VegaFusionCallable::LocalTransform(QUARTER_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "month".to_string(),
        VegaFusionCallable::LocalTransform(MONTH_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "day".to_string(),
        VegaFusionCallable::LocalTransform(DAY_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "date".to_string(),
        VegaFusionCallable::LocalTransform(DATE_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "dayofyear".to_string(),
        VegaFusionCallable::LocalTransform(DAYOFYEAR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "hours".to_string(),
        VegaFusionCallable::LocalTransform(HOUR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "minutes".to_string(),
        VegaFusionCallable::LocalTransform(MINUTE_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "seconds".to_string(),
        VegaFusionCallable::LocalTransform(SECOND_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "milliseconds".to_string(),
        VegaFusionCallable::LocalTransform(MILLISECOND_TRANSFORM.deref().clone()),
    );

    // UTC
    callables.insert(
        "utcyear".to_string(),
        VegaFusionCallable::UtcTransform(UTCYEAR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcquarter".to_string(),
        VegaFusionCallable::UtcTransform(UTCQUARTER_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcmonth".to_string(),
        VegaFusionCallable::UtcTransform(UTCMONTH_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcday".to_string(),
        VegaFusionCallable::UtcTransform(UTCDAY_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcdate".to_string(),
        VegaFusionCallable::UtcTransform(UTCDATE_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcdayofyear".to_string(),
        VegaFusionCallable::UtcTransform(UTCDAYOFYEAR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utchours".to_string(),
        VegaFusionCallable::UtcTransform(UTCHOUR_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcminutes".to_string(),
        VegaFusionCallable::UtcTransform(UTCMINUTE_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcseconds".to_string(),
        VegaFusionCallable::UtcTransform(UTCSECOND_TRANSFORM.deref().clone()),
    );
    callables.insert(
        "utcmilliseconds".to_string(),
        VegaFusionCallable::UtcTransform(UTCMILLISECOND_TRANSFORM.deref().clone()),
    );

    // date time
    callables.insert(
        "datetime".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(datetime_transform_fn)),
    );

    callables.insert(
        "utc".to_string(),
        VegaFusionCallable::UtcTransform(Arc::new(make_datetime_components_fn)),
    );

    callables.insert(
        "time".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(time_fn)),
    );
    callables.insert(
        "timeFormat".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(time_format_fn)),
    );
    callables.insert(
        "utcFormat".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(utc_format_fn)),
    );
    callables.insert(
        "timeOffset".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(time_offset_fn)),
    );

    // format
    callables.insert(
        "format".to_string(),
        VegaFusionCallable::Transform(Arc::new(format_transform)),
    );

    // coercion
    callables.insert(
        "toBoolean".to_string(),
        VegaFusionCallable::Transform(Arc::new(to_boolean_transform)),
    );
    callables.insert(
        "toDate".to_string(),
        VegaFusionCallable::LocalTransform(Arc::new(to_date_transform)),
    );
    callables.insert(
        "toNumber".to_string(),
        VegaFusionCallable::Transform(Arc::new(to_number_transform)),
    );
    callables.insert(
        "toString".to_string(),
        VegaFusionCallable::Transform(Arc::new(to_string_transform)),
    );

    // data
    callables.insert(
        "data".to_string(),
        VegaFusionCallable::Data(Arc::new(data_fn)),
    );

    callables.insert(
        "vlSelectionTest".to_string(),
        VegaFusionCallable::Data(Arc::new(vl_selection_test_fn)),
    );

    callables.insert(
        "vlSelectionResolve".to_string(),
        VegaFusionCallable::Data(Arc::new(vl_selection_resolve_fn)),
    );

    callables
}

#[cfg(test)]
mod tests {
    use crate::expression::compiler::call::{default_callables, VegaFusionCallable};
    use crate::expression::compiler::compile;
    use crate::expression::compiler::config::CompilationConfig;
    use crate::expression::compiler::utils::ExprHelpers;
    use std::collections::HashMap;
    use std::sync::Arc;
    use vegafusion_common::arrow::array::Float64Array;
    use vegafusion_common::data::scalar::ScalarValueHelpers;
    use vegafusion_common::datafusion_common::ScalarValue;
    use vegafusion_core::expression::parser::parse;
    use vegafusion_core::spec::scale::ScaleTypeSpec;
    use vegafusion_core::task_graph::scale_state::ScaleState;

    #[test]
    fn test_scale_and_invert_callables_registered() {
        let callables = default_callables();
        assert!(matches!(
            callables.get("scale"),
            Some(VegaFusionCallable::Scale { invert: false })
        ));
        assert!(matches!(
            callables.get("invert"),
            Some(VegaFusionCallable::Scale { invert: true })
        ));
    }

    #[test]
    fn test_scale_returns_null_for_unknown_scale() {
        let expr = parse("scale('missing', 5)").unwrap();
        let compiled = compile(&expr, &CompilationConfig::default(), None).unwrap();
        let result = compiled.eval_to_scalar().unwrap();
        assert!(matches!(result, ScalarValue::Null));
    }

    #[test]
    fn test_scale_group_arg_not_supported() {
        let expr = parse("scale('x', 5, 0)").unwrap();
        let err = compile(&expr, &CompilationConfig::default(), None).unwrap_err();
        assert!(format!("{err}").contains("optional group argument"));
    }

    #[cfg(feature = "scales")]
    #[test]
    fn test_scale_linear_executes() {
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

        let expr = parse("scale('x', 2)").unwrap();
        let compiled = compile(&expr, &config, None).unwrap();
        let result = compiled.eval_to_scalar().unwrap();
        assert_eq!(result.to_f64().unwrap(), 20.0);
    }

    #[cfg(feature = "scales")]
    #[test]
    fn test_invert_interval_executes() {
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

        let expr = parse("invert('x', [20, 80])").unwrap();
        let compiled = compile(&expr, &config, None).unwrap();
        let result = compiled.eval_to_scalar().unwrap();
        assert_eq!(result.to_f64x2().unwrap(), [2.0, 8.0]);
    }

    #[cfg(feature = "scales")]
    #[test]
    fn test_scale_unsupported_type_returns_explicit_error() {
        let mut config = CompilationConfig::default();
        config.scale_scope.insert(
            "x".to_string(),
            ScaleState {
                scale_type: ScaleTypeSpec::Threshold,
                domain: Arc::new(Float64Array::from(vec![0.0, 1.0])),
                range: Arc::new(Float64Array::from(vec![0.0, 100.0])),
                options: HashMap::new(),
            },
        );

        let expr = parse("scale('x', 0.5)").unwrap();
        let err = compile(&expr, &config, None).unwrap_err();
        assert!(format!("{err}").contains("Scale type not supported"));
    }
}
