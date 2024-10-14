use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::{CompareSpec, Field};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use vegafusion_common::escape::unescape_field;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowTransformSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<CompareSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub groupby: Option<Vec<Field>>,

    pub ops: Vec<WindowTransformOpSpec>,

    pub fields: Vec<Option<Field>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<Value>>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<[Value; 2]>,

    #[serde(rename = "ignorePeers", skip_serializing_if = "Option::is_none")]
    pub ignore_peers: Option<bool>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum WindowOpSpec {
    #[serde(rename = "row_number")]
    RowNumber,
    Rank,

    #[serde(rename = "dense_rank")]
    DenseRank,

    #[serde(rename = "percent_rank")]
    PercentileRank,

    #[serde(rename = "cume_dist")]
    CumeDist,
    NTile,
    Lag,
    Lead,

    #[serde(rename = "first_value")]
    FirstValue,

    #[serde(rename = "last_value")]
    LastValue,

    #[serde(rename = "nth_value")]
    NthValue,

    #[serde(rename = "prev_value")]
    PrevValue,

    #[serde(rename = "next_value")]
    NextValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WindowTransformOpSpec {
    Aggregate(AggregateOpSpec),
    Window(WindowOpSpec),
}

impl TransformSpecTrait for WindowTransformSpec {
    fn supported(&self) -> bool {
        // Check for supported aggregation op
        use AggregateOpSpec::*;
        use WindowOpSpec::*;
        for op in &self.ops {
            match op {
                WindowTransformOpSpec::Aggregate(op) => {
                    if !matches!(
                        op,
                        Count
                            | Sum
                            | Mean
                            | Average
                            | Min
                            | Max
                            | Values
                            | Variance
                            | Variancep
                            | Stdev
                            | Stdevp
                            | Q1
                            | Q3
                    ) {
                        // Unsupported aggregation op
                        return false;
                    }
                }
                WindowTransformOpSpec::Window(op) => {
                    if !matches!(
                        op,
                        RowNumber
                            | Rank
                            | DenseRank
                            | PercentileRank
                            | CumeDist
                            | FirstValue
                            | LastValue
                    ) {
                        // Unsupported window op
                        return false;
                    }
                }
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

            // Compute used columns (both groupby, fields, and sort)
            let mut usage_cols: Vec<_> = self
                .groupby
                .clone()
                .unwrap_or_default()
                .iter()
                .map(|field| unescape_field(&field.field()))
                .collect();
            for field in self.fields.iter().flatten() {
                // Ignore empty fields, which vega-lite sometimes produces instead of null
                if !field.field().trim().is_empty() {
                    usage_cols.push(unescape_field(&field.field()))
                }
            }
            if let Some(sort) = &self.sort {
                let unescaped_sort_fields: Vec<_> = sort
                    .field
                    .to_vec()
                    .iter()
                    .map(|f| unescape_field(f))
                    .collect();
                usage_cols.extend(unescaped_sort_fields)
            }

            let col_usage = ColumnUsage::from(usage_cols.as_slice());
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);
            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // Keep input local datetime columns as window passes through all input columns
        // and doesn't create any local datetime columns
        Vec::from(input_local_datetime_columns)
    }
}
