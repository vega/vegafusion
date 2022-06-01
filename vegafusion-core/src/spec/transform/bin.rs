/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::expression::parser::parse;

use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::spec::values::{Field, SignalExpressionSpec};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinTransformSpec {
    pub field: Field,
    pub extent: BinExtent,
    pub signal: Option<String>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    // Bin configuration parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxbins: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<f64>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<BinSpan>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub minstep: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub divide: Option<Vec<f64>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub nice: Option<bool>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BinExtent {
    Value([f64; 2]),
    Signal(SignalExpressionSpec),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BinSpan {
    Value(f64),
    Signal(SignalExpressionSpec),
}

impl TransformSpecTrait for BinTransformSpec {
    fn supported(&self) -> bool {
        // Check extent expression
        if let BinExtent::Signal(extent) = &self.extent {
            if let Ok(expression) = parse(&extent.signal) {
                if !expression.is_supported() {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check span expression
        if let Some(BinSpan::Signal(span)) = &self.span {
            if let Ok(expression) = parse(&span.signal) {
                if !expression.is_supported() {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let mut input_vars: HashSet<InputVariable> = HashSet::new();
        if let BinExtent::Signal(extent) = &self.extent {
            let expression = parse(&extent.signal)?;
            input_vars.extend(expression.input_vars())
        }

        if let Some(BinSpan::Signal(span)) = &self.span {
            let expression = parse(&span.signal)?;
            input_vars.extend(expression.input_vars())
        }

        Ok(input_vars.into_iter().collect())
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
                .as_ref()
                .and_then(|as_| as_.get(0).cloned())
                .unwrap_or_else(|| "bin0".to_string());
            let bin_end = self
                .as_
                .as_ref()
                .and_then(|as_| as_.get(1).cloned())
                .unwrap_or_else(|| "bin1".to_string());
            let produced = ColumnUsage::from(vec![bin_start, bin_end].as_slice());

            // Compute used columns
            let field = self.field.field();
            let col_usage = ColumnUsage::empty().with_column(&field);
            let usage = DatasetsColumnUsage::empty().with_column_usage(datum_var, col_usage);

            TransformColumns::PassThrough { usage, produced }
        } else {
            TransformColumns::Unknown
        }
    }
}
