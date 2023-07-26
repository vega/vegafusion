use crate::spec::transform::{TransformSpec, TransformSpecTrait};
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::error::Result;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FacetTransformSpec {
    pub groupby: Vec<String>,
    pub transform: Vec<TransformSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for FacetTransformSpec {
    fn supported(&self) -> bool {
        self.transform.iter().all(|tx| tx.supported())
    }

    fn output_signals(&self) -> Vec<String> {
        self.transform
            .iter()
            .flat_map(|tx| tx.output_signals())
            .collect()
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let t = self
            .transform
            .iter()
            .map(|tx| tx.input_vars())
            .collect::<Result<Vec<Vec<_>>>>()?;
        Ok(t.into_iter().flatten().collect())
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        let mut input_local_datetime_columns = Vec::from(input_local_datetime_columns);
        for tx in &self.transform {
            input_local_datetime_columns =
                tx.local_datetime_columns_produced(input_local_datetime_columns.as_slice())
        }
        input_local_datetime_columns
    }
}
