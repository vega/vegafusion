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
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeUnitTransformSpec {
    pub field: String, // TODO: support field object

    #[serde(skip_serializing_if = "Option::is_none")]
    pub units: Option<Vec<TimeUnitUnitSpec>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<TimeUnitTimeZoneSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub extent: Option<(String, String)>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxbins: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnitTimeZoneSpec {
    Local,
    Utc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnitUnitSpec {
    Year,
    Quarter,
    Month,
    Date,
    Week,
    Day,
    DayOfYear,
    Hours,
    Minutes,
    Seconds,
    Milliseconds,
}

impl TransformSpecTrait for TimeUnitTransformSpec {
    fn supported(&self) -> bool {
        !(self.units.is_none()
            || self.step.is_some()
            || self.extent.is_some()
            || self.maxbins.is_some()
            || self.signal.is_some())
    }

    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
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
            let bin_start = self
                .as_
                .clone()
                .and_then(|as_| as_.get(0).cloned())
                .unwrap_or_else(|| "unit0".to_string());
            let mut produced_cols = vec![bin_start];

            if self.interval.unwrap_or(true) {
                let bin_end = self
                    .as_
                    .clone()
                    .and_then(|as_| as_.get(1).cloned())
                    .unwrap_or_else(|| "unit1".to_string());
                produced_cols.push(bin_end)
            }

            let produced = ColumnUsage::from(produced_cols.as_slice());

            // Compute used columns
            let field = self.field.clone();
            let col_usage = ColumnUsage::empty().with_column(&field);
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);

            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }
}
