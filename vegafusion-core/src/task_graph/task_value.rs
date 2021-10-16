use crate::data::table::VegaFusionTable;
use crate::proto::gen::tasks::TaskValue as ProtoTaskValue;
use std::convert::TryFrom;
use crate::error::VegaFusionError;
use crate::proto::gen::tasks::task_value::Data;
use crate::data::scalar::ScalarValue;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub enum TaskValue {
    Scalar(ScalarValue),
    Table(VegaFusionTable),
}

impl TryFrom<&ProtoTaskValue> for TaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &ProtoTaskValue) -> Result<Self, Self::Error> {
        match value.data.as_ref().unwrap() {
            Data::Table(value) => {
                Ok(Self::Table(VegaFusionTable::from_ipc_bytes(value)?))
            }
            Data::Scalar(value) => {
                let scalar_table = VegaFusionTable::from_ipc_bytes(value)?;
                let scalar_rb = scalar_table.to_record_batch()?;
                let scalar_array = scalar_rb.column(0);
                let scalar = ScalarValue::try_from_array(scalar_array, 0)?;
                Ok(Self::Scalar(scalar))
            }
        }
    }
}

impl TryFrom<&TaskValue> for ProtoTaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &TaskValue) -> Result<Self, Self::Error> {
        match value {
            TaskValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array();
                let scalar_rb = RecordBatch::try_from_iter(vec![("value", scalar_array)])?;
                let ipc_bytes = VegaFusionTable::from(scalar_rb).to_ipc_bytes()?;
                Ok(Self {
                    data: Some(Data::Scalar(ipc_bytes))
                })
            }
            TaskValue::Table(table) => {
                Ok(Self {
                    data: Some(Data::Scalar(table.to_ipc_bytes()?))
                })
            }
        }
    }
}
