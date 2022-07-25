/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateTransformSpec {
    pub groupby: Vec<Field>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<Option<Field>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops: Option<Vec<AggregateOpSpec>>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cross: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Field>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOpSpec {
    Count,
    Valid,
    Missing,
    Distinct,
    Sum,
    Product,
    Mean,
    Average,
    Variance,
    Variancep,
    Stdev,
    Stdevp,
    Stderr,
    Median,
    Q1,
    Q3,
    Ci0,
    Ci1,
    Min,
    Max,
    Argmin,
    Argmax,
    Values,
}

impl AggregateOpSpec {
    pub fn name(&self) -> String {
        serde_json::to_value(self)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }
}

impl TransformSpecTrait for AggregateTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOpSpec::*;
        let ops = self.ops.clone().unwrap_or_else(|| vec![Count]);
        for op in &ops {
            if !matches!(
                op,
                Count
                    | Valid
                    | Missing
                    | Distinct
                    | Sum
                    | Mean
                    | Average
                    | Min
                    | Max
                    | Variance
                    | Variancep
                    | Stdev
                    | Stdevp
            ) {
                // Unsupported aggregation op
                return false;
            }
        }

        // Cross aggregation not supported
        if let Some(true) = &self.cross {
            return false;
        }

        // drop=false not support
        if let Some(false) = &self.drop {
            return false;
        }
        true
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        if let Some(datum_var) = datum_var {
            // Compute produced columns
            // Only handle the case where "as" contains a list of strings with length matching ops
            let ops = self
                .ops
                .clone()
                .unwrap_or_else(|| vec![AggregateOpSpec::Count]);
            let as_: Vec<_> = self
                .as_
                .clone()
                .unwrap_or_default()
                .iter()
                .cloned()
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();
            let produced = if ops.len() == as_.len() {
                ColumnUsage::from(as_.as_slice())
            } else {
                ColumnUsage::Unknown
            };

            // Compute used columns (both groupby and fields)
            let mut usage_cols: Vec<_> = self.groupby.iter().map(|field| field.field()).collect();
            for field in self
                .fields
                .clone()
                .unwrap_or_default()
                .into_iter()
                .flatten()
            {
                usage_cols.push(field.field())
            }
            let col_usage = ColumnUsage::from(usage_cols.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::Overwrite { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }
}
