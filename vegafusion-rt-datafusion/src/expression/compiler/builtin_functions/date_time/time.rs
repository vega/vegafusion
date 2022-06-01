/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::builtin_functions::date_time::process_input_datetime;
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;
use vegafusion_core::error::Result;

pub fn time_fn(tz_config: &RuntimeTzConfig, args: &[Expr], _schema: &DFSchema) -> Result<Expr> {
    Ok(Expr::ScalarUDF {
        fun: Arc::new(make_time_udf(tz_config.default_input_tz)),
        args: Vec::from(args),
    })
}

pub fn make_time_udf(default_input_tz: chrono_tz::Tz) -> ScalarUDF {
    let time_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        let arg = process_input_datetime(arg, &default_input_tz);
        Ok(arg)
    };
    let time_fn = make_scalar_function(time_fn);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    ScalarUDF::new(
        "time",
        &Signature::uniform(
            1,
            vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Date32,
                DataType::Date64,
                DataType::Int64,
                DataType::Float64,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &time_fn,
    )
}
