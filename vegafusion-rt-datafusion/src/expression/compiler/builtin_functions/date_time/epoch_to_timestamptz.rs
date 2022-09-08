use datafusion::common::DataFusionError;
use datafusion_expr::{ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, Volatility};
use std::sync::Arc;
use vegafusion_core::arrow::array::{ArrayRef};

use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_core::data::scalar::ScalarValue;
use std::str::FromStr;
use datafusion::arrow::array::Int64Array;
use crate::expression::compiler::builtin_functions::date_time::timestamp_to_timestamptz::millis_to_timestamp;


pub fn make_epoch_to_timestamptz() -> ScalarUDF {
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
            DataFusionError::Internal(format!("Failed to parse {} as a timezone", tz_str))
        })?;

        let millis_array = timestamp_array.as_any().downcast_ref::<Int64Array>().unwrap();
        let timestamp_array = millis_to_timestamp(millis_array, tz);
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

    let signature: Signature = Signature::exact(
        vec![DataType::Int64, DataType::Utf8],
        Volatility::Immutable,
    );

    ScalarUDF::new("epoch_to_timestamptz", &signature, &return_type, &scalar_fn)
}


lazy_static! {
    pub static ref EPOCH_TO_TIMESTAMPTZ_UDF: ScalarUDF = make_epoch_to_timestamptz();
}
