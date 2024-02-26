use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;
use std::any::Any;
use vegafusion_common::datafusion_expr::{ScalarUDFImpl, TypeSignature};
use vegafusion_common::{
    arrow::{
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::ScalarValue,
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility},
};

#[derive(Debug, Clone)]
pub struct UtcTimestampToEpochUDF {
    signature: Signature,
}

impl Default for UtcTimestampToEpochUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl UtcTimestampToEpochUDF {
    pub fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32]),
                TypeSignature::Exact(vec![DataType::Date64]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Millisecond, None)]),
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for UtcTimestampToEpochUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "utc_timestamp_to_epoch_ms"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> vegafusion_common::datafusion_common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
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
    }
}

lazy_static! {
    pub static ref UTC_TIMESTAMP_TO_EPOCH_MS: ScalarUDF =
        ScalarUDF::from(UtcTimestampToEpochUDF::new());
}
