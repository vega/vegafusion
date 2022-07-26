/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::Field;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinAggregateTransformSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    pub fields: Vec<Option<Field>>,
    pub ops: Vec<AggregateOpSpec>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for JoinAggregateTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOpSpec::*;
        for op in &self.ops {
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
            let ops = self.ops.clone();
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
            let mut usage_cols: Vec<_> = self
                .groupby
                .clone()
                .unwrap_or_default()
                .iter()
                .map(|field| field.field())
                .collect();
            for field in self.fields.iter().flatten() {
                usage_cols.push(field.field())
            }
            let col_usage = ColumnUsage::from(usage_cols.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }
}
