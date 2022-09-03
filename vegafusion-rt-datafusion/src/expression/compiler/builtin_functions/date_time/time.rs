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

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_plan::{DFSchema, Expr};

use datafusion::common::DataFusionError;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{
    lit, ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, Signature, TypeSignature,
    Volatility,
};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::error::Result;

pub fn time_fn(tz_config: &RuntimeTzConfig, args: &[Expr], _schema: &DFSchema) -> Result<Expr> {
    let mut udf_args = vec![lit(tz_config.default_input_tz.to_string())];
    udf_args.extend(Vec::from(args));

    Ok(Expr::ScalarUDF {
        fun: Arc::new((*TIME_UDF).clone()),
        args: udf_args,
    })
}

pub fn make_time_udf2() -> ScalarUDF {
    let time_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] default input timezone string
        let default_input_tz = if let ColumnarValue::Scalar(default_input_tz) = &args[0] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected default_input_tz to be a scalar".to_string(),
            ));
        };
        let default_input_tz = chrono_tz::Tz::from_str(&default_input_tz).map_err(|_err| {
            DataFusionError::Internal(format!(
                "Failed to parse {} as a timezone",
                default_input_tz
            ))
        })?;

        // [1] data array
        let data_array = match &args[1] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        let result_array = process_input_datetime(&data_array, &default_input_tz);

        // maybe back to scalar
        if result_array.len() != 1 {
            Ok(ColumnarValue::Array(result_array))
        } else {
            ScalarValue::try_from_array(&result_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    let signature: Signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Date32]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Date64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Float64]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("vg_time", &signature, &return_type, &time_fn)
}

lazy_static! {
    pub static ref TIME_UDF: ScalarUDF = make_time_udf2();
}
