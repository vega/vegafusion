use std::sync::Arc;
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

fn make_timestamptz_to_epoch_ms_udf() -> ScalarUDF {
    let time_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

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
    let signature: Signature = Signature::exact(
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        Volatility::Immutable,
    );

    ScalarUDF::new(
        "timestamptz_to_epoch_ms",
        &signature,
        &return_type,
        &time_fn,
    )
}

lazy_static! {
    pub static ref TIMESTAMPTZ_TO_EPOCH_MS: ScalarUDF = make_timestamptz_to_epoch_ms_udf();
}
