use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::timeunit::to_timestamp_col;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{interval_datetime_lit, interval_year_month_lit, Expr};
use std::ops::Add;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::error::VegaFusionError;

pub fn time_offset_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    schema: &DFSchema,
) -> vegafusion_common::error::Result<Expr> {
    if args.len() < 2 || args.len() > 3 {
        return Err(VegaFusionError::compilation(format!(
            "The timeOffset function accepts either 2 or 3 arguments: received {}",
            args.len()
        )));
    }

    let Expr::Literal(ScalarValue::Utf8(Some(unit))) = &args[0] else {
        return Err(VegaFusionError::compilation(format!(
            "The first argument to the timeOffset function must be a string: received {:?}",
            args[0]
        )));
    };

    let timestamp = &args[1];

    let step = if let Some(step_arg) = args.get(2) {
        let make_err = || {
            Err(VegaFusionError::compilation(format!(
                "The third argument to the timeOffset function must be an integer literal: received {:?}", step_arg
            )))
        };

        // Check for negative integer
        if let Expr::Negative(inner) = step_arg {
            if let Expr::Literal(scalar_value) = inner.as_ref() {
                let dtype = scalar_value.data_type();
                if dtype.is_integer() {
                    // Negate inner integer
                    scalar_value.negate().to_i32()?
                } else if dtype.is_floating() {
                    let step_float = scalar_value.to_f64()?;
                    if step_float.fract() == 0.0 {
                        // cast to negative integer literal
                        -step_float as i32
                    } else {
                        return make_err();
                    }
                } else {
                    return make_err();
                }
            } else {
                return make_err();
            }
        } else if let Expr::Literal(scalar_value) = step_arg {
            let dtype = scalar_value.data_type();
            if dtype.is_integer() {
                scalar_value.clone().to_i32()?
            } else if dtype.is_floating() {
                let step_float = scalar_value.to_f64()?;
                if step_float.fract() == 0.0 {
                    // cast to integer literal
                    step_float as i32
                } else {
                    return make_err();
                }
            } else {
                return make_err();
            }
        } else {
            return make_err();
        }
    } else {
        1
    };

    let timestamp = to_timestamp_col(
        timestamp.clone(),
        schema,
        &tz_config.default_input_tz.to_string(),
    )?;
    let interval = match unit.to_lowercase().as_str() {
        unit @ ("year" | "month") => interval_year_month_lit(&format!("{step} {unit}")),
        "quarter" => interval_year_month_lit(&format!("{} month", step * 3)),
        unit => interval_datetime_lit(&format!("{step} {unit}")),
    };

    Ok(timestamp.add(interval))
}
