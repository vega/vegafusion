/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::proto::gen::transforms::{
    window_transform_op, AggregateOp, SortOrder, Window, WindowFrame, WindowOp, WindowTransformOp,
};
use crate::spec::transform::aggregate::AggregateOpSpec;
use crate::spec::transform::window::{WindowOpSpec, WindowTransformOpSpec, WindowTransformSpec};
use crate::spec::values::SortOrderSpec;
use crate::transform::TransformDependencies;
use serde_json::Value;

impl Window {
    pub fn try_new(transform: &WindowTransformSpec) -> Result<Self> {
        // Process frame
        let frame = transform.frame.as_ref().map(|f| {
            let frame: Vec<_> = f
                .iter()
                .map(|_v| match f.get(0).unwrap() {
                    Value::Number(n) => n.as_i64(),
                    _ => None,
                })
                .collect();

            WindowFrame {
                start: frame[0],
                end: frame[1],
            }
        });

        // Process sorting
        let (sort_fields, sort) = match &transform.sort {
            Some(sort) => {
                let fields = sort.field.to_vec();
                let order: Vec<_> = match &sort.order {
                    Some(order) => order
                        .to_vec()
                        .iter()
                        .map(|ord| match ord {
                            SortOrderSpec::Descending => SortOrder::Descending as i32,
                            SortOrderSpec::Ascending => SortOrder::Ascending as i32,
                        })
                        .collect(),
                    None => {
                        vec![SortOrder::Ascending as i32; fields.len()]
                    }
                };
                (fields, order)
            }
            None => (Vec::new(), Vec::new()),
        };

        // Process fields (Convert null/None to empty string)
        let fields: Vec<_> = transform
            .fields
            .iter()
            .map(|f| {
                f.as_ref()
                    .map(|f| f.field())
                    .unwrap_or_else(|| "".to_string())
            })
            .collect();

        // Process groupby
        let groupby: Vec<_> = transform
            .groupby
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|f| f.field())
            .collect();

        // Process ops
        let ops: Vec<_> = transform
            .ops
            .iter()
            .map(|tx_op| match tx_op {
                WindowTransformOpSpec::Aggregate(op_spec) => {
                    let op = match op_spec {
                        AggregateOpSpec::Count => AggregateOp::Count,
                        AggregateOpSpec::Valid => AggregateOp::Valid,
                        AggregateOpSpec::Missing => AggregateOp::Missing,
                        AggregateOpSpec::Distinct => AggregateOp::Distinct,
                        AggregateOpSpec::Sum => AggregateOp::Sum,
                        AggregateOpSpec::Product => AggregateOp::Product,
                        AggregateOpSpec::Mean => AggregateOp::Mean,
                        AggregateOpSpec::Average => AggregateOp::Average,
                        AggregateOpSpec::Variance => AggregateOp::Variance,
                        AggregateOpSpec::Variancep => AggregateOp::Variancep,
                        AggregateOpSpec::Stdev => AggregateOp::Stdev,
                        AggregateOpSpec::Stdevp => AggregateOp::Stdevp,
                        AggregateOpSpec::Stderr => AggregateOp::Stderr,
                        AggregateOpSpec::Median => AggregateOp::Median,
                        AggregateOpSpec::Q1 => AggregateOp::Q1,
                        AggregateOpSpec::Q3 => AggregateOp::Q3,
                        AggregateOpSpec::Ci0 => AggregateOp::Ci0,
                        AggregateOpSpec::Ci1 => AggregateOp::Ci1,
                        AggregateOpSpec::Min => AggregateOp::Min,
                        AggregateOpSpec::Max => AggregateOp::Max,
                        AggregateOpSpec::Argmin => AggregateOp::Argmin,
                        AggregateOpSpec::Argmax => AggregateOp::Argmax,
                        AggregateOpSpec::Values => AggregateOp::Values,
                    };
                    WindowTransformOp {
                        op: Some(window_transform_op::Op::AggregateOp(op as i32)),
                    }
                }
                WindowTransformOpSpec::Window(op_spec) => {
                    let op = match op_spec {
                        WindowOpSpec::RowNumber => WindowOp::RowNumber,
                        WindowOpSpec::Rank => WindowOp::Rank,
                        WindowOpSpec::DenseRank => WindowOp::DenseRank,
                        WindowOpSpec::PercentileRank => WindowOp::PercentileRank,
                        WindowOpSpec::CumeDist => WindowOp::CumeDist,
                        WindowOpSpec::NTile => WindowOp::NTile,
                        WindowOpSpec::Lag => WindowOp::Lag,
                        WindowOpSpec::Lead => WindowOp::Lead,
                        WindowOpSpec::FirstValue => WindowOp::FirstValue,
                        WindowOpSpec::LastValue => WindowOp::LastValue,
                        WindowOpSpec::NthValue => WindowOp::NthValue,
                        WindowOpSpec::PrevValue => WindowOp::PrevValue,
                        WindowOpSpec::NextValue => WindowOp::NextValue,
                    };

                    WindowTransformOp {
                        op: Some(window_transform_op::Op::WindowOp(op as i32)),
                    }
                }
            })
            .collect();

        // Process aliases. Convert None to empty string
        let aliases: Vec<_> = transform
            .as_
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|alias| alias.clone().unwrap_or_else(|| "".to_string()))
            .collect();

        // Process params
        let params: Vec<_> = transform
            .params
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|param| param.as_f64().unwrap_or(f64::NAN))
            .collect();

        Ok(Self {
            sort,
            sort_fields,
            groupby,
            ops,
            fields,
            params,
            aliases,
            frame,
        })
    }
}

impl TransformDependencies for Window {}
