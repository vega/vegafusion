use std::any::Any;
use vegafusion_common::datafusion_expr::ScalarUDFImpl;
use vegafusion_common::{
    arrow::{
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::ScalarValue,
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility},
};

#[derive(Debug, Clone)]
pub struct EpochMsToUtcTimestampUDF {
    signature: Signature,
}

impl Default for EpochMsToUtcTimestampUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl EpochMsToUtcTimestampUDF {
    pub fn new() -> Self {
        let signature = Signature::exact(vec![DataType::Int64], Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for EpochMsToUtcTimestampUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "epoch_ms_to_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> vegafusion_common::datafusion_common::Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        // [0] data array
        let timestamp_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
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
    }
}

lazy_static! {
    pub static ref EPOCH_MS_TO_UTC_TIMESTAMP_UDF: ScalarUDF =
        ScalarUDF::from(EpochMsToUtcTimestampUDF::new());
}
