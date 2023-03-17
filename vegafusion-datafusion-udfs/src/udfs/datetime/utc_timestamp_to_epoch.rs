use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;
use std::sync::Arc;
use vegafusion_common::datafusion_expr::TypeSignature;
use vegafusion_common::{
    arrow::{
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::ScalarValue,
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_utc_timestamp_to_epoch_ms_udf() -> ScalarUDF {
    let time_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };
        let data_array = to_timestamp_ms(&data_array)?;

        // cast timestamp millis to Int64
        let result_array = cast(&data_array, &DataType::Int64)?;

        // maybe back to scalar
        if result_array.len() != 1 {
            Ok(ColumnarValue::Array(result_array))
        } else {
            ScalarValue::try_from_array(&result_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    let signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Date32]),
            TypeSignature::Exact(vec![DataType::Date64]),
            TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Millisecond, None)]),
            TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "utc_timestamp_to_epoch_ms",
        &signature,
        &return_type,
        &time_fn,
    )
}

lazy_static! {
    pub static ref UTC_TIMESTAMP_TO_EPOCH_MS: ScalarUDF = make_utc_timestamp_to_epoch_ms_udf();
}
