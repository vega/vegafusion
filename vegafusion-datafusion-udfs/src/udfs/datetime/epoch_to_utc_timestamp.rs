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

fn make_epoch_to_utc_timestamp() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        let timestamp_array = cast(
            &timestamp_array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;

        // maybe back to scalar
        if timestamp_array.len() != 1 {
            Ok(ColumnarValue::Array(timestamp_array))
        } else {
            ScalarValue::try_from_array(&timestamp_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature: Signature = Signature::exact(vec![DataType::Int64], Volatility::Immutable);

    ScalarUDF::new(
        "epoch_ms_to_utc_timestamp",
        &signature,
        &return_type,
        &scalar_fn,
    )
}

lazy_static! {
    pub static ref EPOCH_MS_TO_UTC_TIMESTAMP_UDF: ScalarUDF = make_epoch_to_utc_timestamp();
}
