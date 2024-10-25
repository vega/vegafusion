use std::ops::Add;
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::{expr, lit, Expr, ExprSchemable};
use std::sync::Arc;
use datafusion_functions::expr_fn::{date_part, to_unixtime};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{cast_to, is_numeric_datatype};
use vegafusion_common::error::{Result, VegaFusionError};
use crate::transform::utils::{str_to_timestamp, to_epoch_millis};

pub fn time_fn(tz_config: &RuntimeTzConfig, args: &[Expr], schema: &DFSchema) -> Result<Expr> {
    // Validate number of arguments
    if args.len() != 1 {
        return Err(VegaFusionError::compilation(format!(
            "Expected a single argument to time function: received {}",
            args.len()
        )));
    }

    // Extract first and only arg
    to_epoch_millis(args[0].clone(), &tz_config.default_input_tz.to_string(), schema)
}
