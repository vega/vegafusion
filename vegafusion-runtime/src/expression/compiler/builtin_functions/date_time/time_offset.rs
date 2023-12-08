use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{expr, lit, Expr};
use std::sync::Arc;
use vegafusion_common::error::VegaFusionError;
use vegafusion_datafusion_udfs::udfs::datetime::date_add_tz::DATE_ADD_TZ_UDF;

pub fn time_offset_fn(
    tz_config: &RuntimeTzConfig,
    args: &[Expr],
    _schema: &DFSchema,
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
    let step = args.get(2).cloned().unwrap_or_else(|| lit(1.0));

    let mut udf_args = vec![lit(tz_config.local_tz.to_string())];
    udf_args.extend(Vec::from(args));
    Ok(Expr::ScalarUDF(expr::ScalarUDF {
        fun: Arc::new((*DATE_ADD_TZ_UDF).clone()),
        args: vec![
            lit(unit),
            step,
            timestamp.clone(),
            lit(tz_config.local_tz.to_string()),
        ],
    }))
}
