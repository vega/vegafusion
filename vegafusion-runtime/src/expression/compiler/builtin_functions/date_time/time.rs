use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::utils::to_epoch_millis;
use datafusion_expr::Expr;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::error::{Result, VegaFusionError};

pub fn time_fn(tz_config: &RuntimeTzConfig, args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    // Validate number of arguments
    if args.len() != 1 {
        return Err(VegaFusionError::compilation(format!(
            "Expected a single argument to time function: received {}",
            args.len()
        )));
    }

    // Extract first and only arg
    to_epoch_millis(
        args[0].clone(),
        &tz_config.default_input_tz.to_string(),
        schema,
    )
}
