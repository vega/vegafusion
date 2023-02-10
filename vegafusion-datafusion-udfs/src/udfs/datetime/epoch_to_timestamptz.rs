use crate::udfs::datetime::timestamp_to_timestamptz::convert_timezone;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, TimestampMillisecondArray},
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_epoch_to_timestamptz() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        // [1] timezone string
        let tz_str = if let ColumnarValue::Scalar(default_input_tz) = &args[1] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected default_input_tz to be a scalar".to_string(),
            ));
        };
        let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
            DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
        })?;

        let timestamp_millis = cast(
            &timestamp_array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;
        let timestamp_millis = timestamp_millis
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let timestamp_array = convert_timezone(timestamp_millis, tz);
        let timestamp_array = Arc::new(timestamp_array) as ArrayRef;

        // maybe back to scalar
        if timestamp_array.len() != 1 {
            Ok(ColumnarValue::Array(timestamp_array))
        } else {
            ScalarValue::try_from_array(&timestamp_array, 0).map(ColumnarValue::Scalar)
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature: Signature =
        Signature::exact(vec![DataType::Int64, DataType::Utf8], Volatility::Immutable);

    ScalarUDF::new(
        "epoch_ms_to_timestamptz",
        &signature,
        &return_type,
        &scalar_fn,
    )
}

lazy_static! {
    pub static ref EPOCH_MS_TO_TIMESTAMPTZ_UDF: ScalarUDF = make_epoch_to_timestamptz();
}
