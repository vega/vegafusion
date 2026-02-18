use datafusion_common::DFSchema;
use datafusion_expr::{expr, lit, when, Expr, ScalarUDF};
use datafusion_functions::math::{abs, exp, ln, power};
use datafusion_functions_nested::expr_fn::{array_element, array_length, make_array};
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datatypes::cast_to;
use vegafusion_core::error::{Result, VegaFusionError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InteractionTransform {
    Linear,
    Log,
    Pow,
    Symlog,
}

pub fn bandspace_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    if args.is_empty() || args.len() > 3 {
        return Err(VegaFusionError::parse(format!(
            "bandspace requires 1 to 3 arguments. Received {} arguments",
            args.len()
        )));
    }

    let count = coerce_number(args[0].clone(), schema)?;
    let padding_inner = args
        .get(1)
        .map(|arg| coerce_number(arg.clone(), schema))
        .transpose()?
        .unwrap_or_else(|| lit(0.0_f64));
    let padding_outer = args
        .get(2)
        .map(|arg| coerce_number(arg.clone(), schema))
        .transpose()?
        .unwrap_or_else(|| lit(0.0_f64));

    let space = count.clone() - padding_inner + lit(2.0_f64) * padding_outer;
    Ok(when(
        count.clone().not_eq(lit(0.0_f64)),
        when(space.clone().gt(lit(0.0_f64)), space).otherwise(lit(1.0_f64))?,
    )
    .otherwise(lit(0.0_f64))?)
}

pub fn pan_linear_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    pan_transform(args, schema, InteractionTransform::Linear)
}

pub fn pan_log_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    pan_transform(args, schema, InteractionTransform::Log)
}

pub fn pan_pow_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    pan_transform(args, schema, InteractionTransform::Pow)
}

pub fn pan_symlog_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    pan_transform(args, schema, InteractionTransform::Symlog)
}

pub fn zoom_linear_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    zoom_transform(args, schema, InteractionTransform::Linear)
}

pub fn zoom_log_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    zoom_transform(args, schema, InteractionTransform::Log)
}

pub fn zoom_pow_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    zoom_transform(args, schema, InteractionTransform::Pow)
}

pub fn zoom_symlog_transform(args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    zoom_transform(args, schema, InteractionTransform::Symlog)
}

fn pan_transform(
    args: &[Expr],
    schema: &DFSchema,
    transform: InteractionTransform,
) -> Result<Expr> {
    let expected_args = if matches!(
        transform,
        InteractionTransform::Linear | InteractionTransform::Log
    ) {
        2
    } else {
        3
    };
    ensure_arg_count(args, expected_args, pan_fn_name(transform))?;

    let domain = args[0].clone();
    let delta = coerce_number(args[1].clone(), schema)?;
    let transform_param = args
        .get(2)
        .map(|expr| coerce_number(expr.clone(), schema))
        .transpose()?;

    let (domain_start_raw, domain_end_raw) = domain_bounds(domain, schema)?;
    let log_sign = if transform == InteractionTransform::Log {
        Some(signum_expr(domain_start_raw.clone(), schema)?)
    } else {
        None
    };

    let domain_start = lift_expr(
        domain_start_raw,
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let domain_end = lift_expr(
        domain_end_raw,
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let domain_delta = (domain_end.clone() - domain_start.clone()) * delta;

    let result_start = ground_expr(
        domain_start - domain_delta.clone(),
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let result_end = ground_expr(
        domain_end - domain_delta,
        transform,
        transform_param,
        log_sign,
        schema,
    )?;

    Ok(make_array(vec![result_start, result_end]))
}

fn zoom_transform(
    args: &[Expr],
    schema: &DFSchema,
    transform: InteractionTransform,
) -> Result<Expr> {
    let expected_args = if matches!(
        transform,
        InteractionTransform::Linear | InteractionTransform::Log
    ) {
        3
    } else {
        4
    };
    ensure_arg_count(args, expected_args, zoom_fn_name(transform))?;

    let domain = args[0].clone();
    let anchor_raw = args[1].clone();
    let scale_factor = coerce_number(args[2].clone(), schema)?;
    let transform_param = args
        .get(3)
        .map(|expr| coerce_number(expr.clone(), schema))
        .transpose()?;

    let (domain_start_raw, domain_end_raw) = domain_bounds(domain, schema)?;
    let log_sign = if transform == InteractionTransform::Log {
        Some(signum_expr(domain_start_raw.clone(), schema)?)
    } else {
        None
    };

    let domain_start = lift_expr(
        domain_start_raw,
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let domain_end = lift_expr(
        domain_end_raw,
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let midpoint = (domain_start.clone() + domain_end.clone()) / lit(2.0_f64);
    let anchor_lifted = lift_expr(
        anchor_raw.clone(),
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let anchor = when(anchor_raw.is_not_null(), anchor_lifted).otherwise(midpoint)?;

    let result_start = ground_expr(
        anchor.clone() + (domain_start.clone() - anchor.clone()) * scale_factor.clone(),
        transform,
        transform_param.clone(),
        log_sign.clone(),
        schema,
    )?;
    let result_end = ground_expr(
        anchor.clone() + (domain_end.clone() - anchor) * scale_factor,
        transform,
        transform_param,
        log_sign,
        schema,
    )?;

    Ok(make_array(vec![result_start, result_end]))
}

fn domain_bounds(domain: Expr, schema: &DFSchema) -> Result<(Expr, Expr)> {
    let start = array_element(domain.clone(), lit(1));
    let end_index = cast_to(array_length(domain.clone()), &DataType::Int64, schema)?;
    let end = array_element(domain.clone(), end_index);
    Ok((start, end))
}

fn lift_expr(
    value: Expr,
    transform: InteractionTransform,
    transform_param: Option<Expr>,
    log_sign: Option<Expr>,
    schema: &DFSchema,
) -> Result<Expr> {
    let value = coerce_number(value, schema)?;
    match transform {
        InteractionTransform::Linear => Ok(value),
        InteractionTransform::Log => {
            let sign = log_sign.ok_or_else(|| {
                VegaFusionError::internal("Missing log-domain sign for pan/zoom log transform")
            })?;
            Ok(call_udf(ln(), vec![sign * value]))
        }
        InteractionTransform::Pow => {
            let exponent = transform_param.ok_or_else(|| {
                VegaFusionError::internal("Missing exponent for panPow/zoomPow transform")
            })?;
            signed_pow(value, exponent, schema)
        }
        InteractionTransform::Symlog => {
            let constant = transform_param.ok_or_else(|| {
                VegaFusionError::internal("Missing constant for panSymlog/zoomSymlog transform")
            })?;
            let sign = signum_expr(value.clone(), schema)?;
            let inner = lit(1.0_f64) + call_udf(abs(), vec![value / constant]);
            Ok(sign * call_udf(ln(), vec![inner]))
        }
    }
}

fn ground_expr(
    value: Expr,
    transform: InteractionTransform,
    transform_param: Option<Expr>,
    log_sign: Option<Expr>,
    schema: &DFSchema,
) -> Result<Expr> {
    let value = coerce_number(value, schema)?;
    match transform {
        InteractionTransform::Linear => Ok(value),
        InteractionTransform::Log => {
            let sign = log_sign.ok_or_else(|| {
                VegaFusionError::internal("Missing log-domain sign for pan/zoom log transform")
            })?;
            Ok(sign * call_udf(exp(), vec![value]))
        }
        InteractionTransform::Pow => {
            let exponent = transform_param.ok_or_else(|| {
                VegaFusionError::internal("Missing exponent for panPow/zoomPow transform")
            })?;
            signed_pow(value, lit(1.0_f64) / exponent, schema)
        }
        InteractionTransform::Symlog => {
            let constant = transform_param.ok_or_else(|| {
                VegaFusionError::internal("Missing constant for panSymlog/zoomSymlog transform")
            })?;
            let sign = signum_expr(value.clone(), schema)?;
            let exp_minus_one =
                call_udf(exp(), vec![call_udf(abs(), vec![value.clone()])]) - lit(1.0_f64);
            Ok(sign * exp_minus_one * constant)
        }
    }
}

fn signed_pow(value: Expr, exponent: Expr, schema: &DFSchema) -> Result<Expr> {
    let value = coerce_number(value, schema)?;
    let exponent = coerce_number(exponent, schema)?;
    let negative_branch = lit(-1.0_f64)
        * call_udf(
            power(),
            vec![lit(-1.0_f64) * value.clone(), exponent.clone()],
        );
    let positive_branch = call_udf(power(), vec![value.clone(), exponent]);
    Ok(when(value.lt(lit(0.0_f64)), negative_branch).otherwise(positive_branch)?)
}

fn signum_expr(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let value = cast_to(value, &DataType::Float64, schema)?;
    Ok(when(value.clone().gt(lit(0.0_f64)), lit(1.0_f64))
        .otherwise(when(value.clone().lt(lit(0.0_f64)), lit(-1.0_f64)).otherwise(lit(0.0_f64))?)?)
}

fn coerce_number(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let casted = cast_to(value, &DataType::Float64, schema)?;
    Ok(when(casted.clone().is_null(), lit(0.0_f64)).otherwise(casted)?)
}

fn call_udf(udf: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(expr::ScalarFunction { func: udf, args })
}

fn ensure_arg_count(args: &[Expr], expected: usize, fn_name: &str) -> Result<()> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(VegaFusionError::parse(format!(
            "{fn_name} requires {expected} arguments. Received {} arguments",
            args.len()
        )))
    }
}

fn pan_fn_name(transform: InteractionTransform) -> &'static str {
    match transform {
        InteractionTransform::Linear => "panLinear",
        InteractionTransform::Log => "panLog",
        InteractionTransform::Pow => "panPow",
        InteractionTransform::Symlog => "panSymlog",
    }
}

fn zoom_fn_name(transform: InteractionTransform) -> &'static str {
    match transform {
        InteractionTransform::Linear => "zoomLinear",
        InteractionTransform::Log => "zoomLog",
        InteractionTransform::Pow => "zoomPow",
        InteractionTransform::Symlog => "zoomSymlog",
    }
}
