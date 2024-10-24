use vegafusion_common::arrow::array::Array;
use vegafusion_common::{
    arrow::{
        array::ArrayRef,
        compute::cast,
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError},
};

pub fn to_timestamp_ms(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::Timestamp(time_unit, _) => {
            if time_unit == &TimeUnit::Millisecond {
                Ok(array.clone())
            } else {
                Ok(cast(
                    array,
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                )?)
            }
        }
        DataType::Date32 => Ok(cast(
            array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?),
        DataType::Date64 => Ok(cast(
            array,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?),
        dtype => Err(DataFusionError::Internal(format!(
            "Unexpected datatime in to_timestamp_ms: {dtype:?}"
        ))),
    }
}
