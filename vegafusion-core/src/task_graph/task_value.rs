use crate::proto::gen::tasks::materialized_task_value::Data as MaterializedTaskValueData;
use crate::proto::gen::tasks::ResponseTaskValue;
use crate::proto::gen::tasks::{
    MaterializedTaskValue as ProtoMaterializedTaskValue, TaskGraphValueResponse, Variable,
};
use crate::runtime::PlanExecutor;
use crate::task_graph::memory::{
    inner_size_of_logical_plan, inner_size_of_scalar, inner_size_of_table,
};
use datafusion_common::ScalarValue;
use serde_json::Value;
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

#[derive(Debug, Clone)]
pub enum TaskValue {
    Scalar(ScalarValue),
    Table(VegaFusionTable),
    Plan(LogicalPlan),
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

    pub fn size_of(&self) -> usize {
        let inner_size = match self {
            TaskValue::Scalar(scalar) => inner_size_of_scalar(scalar),
            TaskValue::Table(table) => inner_size_of_table(table),
            TaskValue::Plan(plan) => inner_size_of_logical_plan(plan),
        };

        std::mem::size_of::<Self>() + inner_size
    }

    pub async fn to_materialized(
        self,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Result<MaterializedTaskValue> {
        match self {
            TaskValue::Plan(plan) => {
                let table = plan_executor.execute_plan(plan).await?;
                Ok(MaterializedTaskValue::Table(table))
            }
            TaskValue::Scalar(scalar) => Ok(MaterializedTaskValue::Scalar(scalar)),
            TaskValue::Table(table) => Ok(MaterializedTaskValue::Table(table)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MaterializedTaskValue {
    Scalar(ScalarValue),
    Table(VegaFusionTable),
}

impl MaterializedTaskValue {
    pub fn as_scalar(&self) -> Result<&ScalarValue> {
        match self {
            MaterializedTaskValue::Scalar(value) => Ok(value),
            _ => Err(VegaFusionError::internal("Value is not a scalar")),
        }
    }

    pub fn as_table(&self) -> Result<&VegaFusionTable> {
        match self {
            MaterializedTaskValue::Table(value) => Ok(value),
            _ => Err(VegaFusionError::internal("Value is not a table")),
        }
    }

    pub fn to_json(&self) -> Result<Value> {
        match self {
            MaterializedTaskValue::Scalar(value) => value.to_json(),
            MaterializedTaskValue::Table(value) => Ok(value.to_json()?),
        }
    }
}

impl TryFrom<&ProtoMaterializedTaskValue> for TaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &ProtoMaterializedTaskValue) -> std::result::Result<Self, Self::Error> {
        match value.data.as_ref().unwrap() {
            MaterializedTaskValueData::Table(value) => {
                Ok(Self::Table(VegaFusionTable::from_ipc_bytes(value)?))
            }
            MaterializedTaskValueData::Scalar(value) => {
                let scalar_table = VegaFusionTable::from_ipc_bytes(value)?;
                let scalar_rb = scalar_table.to_record_batch()?;
                let scalar_array = scalar_rb.column(0);
                let scalar = ScalarValue::try_from_array(scalar_array, 0)?;
                Ok(Self::Scalar(scalar))
            }
        }
    }
}

impl TryFrom<&TaskValue> for ProtoMaterializedTaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &TaskValue) -> std::result::Result<Self, Self::Error> {
        match value {
            TaskValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array()?;
                let scalar_rb = RecordBatch::try_from_iter(vec![("value", scalar_array)])?;
                let ipc_bytes = VegaFusionTable::from(scalar_rb).to_ipc_bytes()?;
                Ok(Self {
                    data: Some(MaterializedTaskValueData::Scalar(ipc_bytes)),
                })
            }
            TaskValue::Table(table) => Ok(Self {
                data: Some(MaterializedTaskValueData::Table(table.to_ipc_bytes()?)),
            }),
            TaskValue::Plan(_) => Err(VegaFusionError::internal(
                "TaskValue::Plan cannot be serialized to protobuf. Plans are intermediate values that should be materialized to tables using .to_materialized(plan_executor) before serialization.",
            )),
        }
    }
}

impl TryFrom<&MaterializedTaskValue> for ProtoMaterializedTaskValue {
    type Error = VegaFusionError;

    fn try_from(value: &MaterializedTaskValue) -> std::result::Result<Self, Self::Error> {
        match value {
            MaterializedTaskValue::Scalar(scalar) => {
                let scalar_array = scalar.to_array()?;
                let scalar_rb = RecordBatch::try_from_iter(vec![("value", scalar_array)])?;
                let ipc_bytes = VegaFusionTable::from(scalar_rb).to_ipc_bytes()?;
                Ok(Self {
                    data: Some(MaterializedTaskValueData::Scalar(ipc_bytes)),
                })
            }
            MaterializedTaskValue::Table(table) => Ok(Self {
                data: Some(MaterializedTaskValueData::Table(table.to_ipc_bytes()?)),
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

impl From<ResponseTaskValue> for NamedTaskValue {
    fn from(value: ResponseTaskValue) -> Self {
        NamedTaskValue {
            variable: value.variable.unwrap(),
            scope: value.scope,
            value: TaskValue::try_from(&value.value.unwrap()).unwrap(),
        }
    }
}
