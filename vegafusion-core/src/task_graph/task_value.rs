use crate::proto::gen::tasks::task_value::Data;
use crate::proto::gen::tasks::ResponseTaskValue;
use crate::proto::gen::tasks::{TaskGraphValueResponse, TaskValue as ProtoTaskValue, Variable};
use crate::task_graph::memory::{inner_size_of_scalar, inner_size_of_table};
use datafusion_common::ScalarValue;
use serde_json::Value;
use std::convert::TryFrom;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

#[derive(Debug, Clone)]
pub enum TaskValue {
    Scalar(ScalarValue),
    Table(VegaFusionTable),
}

impl TaskValue {
    pub fn as_scalar(&self) -> Result<&ScalarValue> {
        match self {
            TaskValue::Scalar(value) => Ok(value),
            _ => Err(VegaFusionError::internal("Value is not a scalar")),
        }
    }

    pub fn as_table(&self) -> Result<&VegaFusionTable> {
        match self {
            TaskValue::Table(value) => Ok(value),
            _ => Err(VegaFusionError::internal("Value is not a table")),
        }
    }

    pub fn to_json(&self) -> Result<Value> {
        match self {
            TaskValue::Scalar(value) => value.to_json(),
            TaskValue::Table(value) => Ok(value.to_json()?),
        }
    }

    pub fn size_of(&self) -> usize {
        let inner_size = match self {
            TaskValue::Scalar(scalar) => inner_size_of_scalar(scalar),
            TaskValue::Table(table) => inner_size_of_table(table),
        };

        std::mem::size_of::<Self>() + inner_size
    }
}

impl TryFrom<&ProtoTaskValue> for TaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &ProtoTaskValue) -> std::result::Result<Self, Self::Error> {
        match value.data.as_ref().unwrap() {
            Data::Table(value) => Ok(Self::Table(VegaFusionTable::from_ipc_bytes(value)?)),
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

    fn try_from(value: &TaskValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TaskValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array()?;
                let scalar_rb = RecordBatch::try_from_iter(vec![("value", scalar_array)])?;
                let ipc_bytes = VegaFusionTable::from(scalar_rb).to_ipc_bytes()?;
                Ok(Self {
                    data: Some(Data::Scalar(ipc_bytes)),
                })
            }
            TaskValue::Table(table) => Ok(Self {
                data: Some(Data::Table(table.to_ipc_bytes()?)),
            }),
        }
    }
}

impl TaskGraphValueResponse {
    pub fn deserialize(self) -> Result<Vec<(Variable, Vec<u32>, TaskValue)>> {
        self.response_values
            .into_iter()
            .map(|response_value| {
                let variable = response_value
                    .variable
                    .with_context(|| "Unwrap failed for variable of response value".to_string())?;

                let scope = response_value.scope;
                let proto_value = response_value.value.with_context(|| {
                    "Unwrap failed for value of response value: {:?}".to_string()
                })?;

                let value = TaskValue::try_from(&proto_value).with_context(|| {
                    "Deserialization failed for value of response value: {:?}".to_string()
                })?;

                Ok((variable, scope, value))
            })
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Debug, Clone)]
pub struct NamedTaskValue {
    pub variable: Variable,
    pub scope: Vec<u32>,
    pub value: TaskValue,
}

impl From<NamedTaskValue> for ResponseTaskValue {
    fn from(value: NamedTaskValue) -> Self {
        ResponseTaskValue {
            variable: Some(value.variable),
            scope: value.scope,
            value: Some(ProtoTaskValue::try_from(&value.value).unwrap()),
        }
    }
}

impl From<ResponseTaskValue> for NamedTaskValue {
    fn from(value: ResponseTaskValue) -> Self {
        NamedTaskValue {
            variable: value.variable.unwrap(),
            scope: value.scope,
            value: TaskValue::try_from(&value.value.unwrap()).unwrap(),
        }
    }
}
