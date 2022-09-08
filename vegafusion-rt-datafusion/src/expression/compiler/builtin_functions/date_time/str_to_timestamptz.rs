use datafusion::common::DataFusionError;
use datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    Volatility,
};
use std::sync::Arc;
use vegafusion_core::arrow::array::StringArray;

use std::str::FromStr;
use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_core::data::scalar::ScalarValue;

use crate::expression::compiler::builtin_functions::date_time::date_parsing::{
    datetime_strs_to_timestamp_millis, DateParseMode,
};

pub fn make_str_to_timestamptz() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [0] data array
        let str_array = match &args[0] {
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

        let str_array = str_array.as_any().downcast_ref::<StringArray>().unwrap();

        let timestamp_array =
            datetime_strs_to_timestamp_millis(str_array, DateParseMode::JavaScript, &Some(tz));

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
        Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable);

    ScalarUDF::new("str_to_timestamptz", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref STR_TO_TIMESTAMPTZ_UDF: ScalarUDF = make_str_to_timestamptz();
}
