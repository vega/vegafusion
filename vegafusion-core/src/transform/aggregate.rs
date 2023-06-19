use crate::proto::gen::transforms::{Aggregate, AggregateOp};
use crate::spec::transform::aggregate::{AggregateOpSpec, AggregateTransformSpec};
use crate::transform::TransformDependencies;
use itertools::Itertools;

impl Aggregate {
    pub fn new(transform: &AggregateTransformSpec) -> Self {
        let tx_fields = transform.fields.clone().unwrap_or_else(|| vec![None]);
        let fields: Vec<_> = tx_fields
            .iter()
            .map(|f| f.as_ref().map(|f| f.field()).unwrap_or_default())
            .collect();

        let groupby: Vec<_> = transform
            .groupby
            .iter()
            .map(|f| f.field())
            .unique()
            .collect();

        // Initialize aliases with those potentially provided in field objects
        // (e.g. {"field": "foo", "as": "bar"}
        let mut aliases: Vec<_> = tx_fields
            .iter()
            .map(|f| f.as_ref().and_then(|f| f.as_()).unwrap_or_default())
            .collect();

        // Overwrite aliases with those provided in the as_ prop of the transform
        for (i, as_) in transform.as_.clone().unwrap_or_default().iter().enumerate() {
            if as_.is_some() {
                aliases[i] = as_.clone().unwrap();
            }
        }

        let ops = transform
            .ops
            .clone()
            .unwrap_or_else(|| vec![AggregateOpSpec::Count]);
        let ops: Vec<_> = ops
            .iter()
            .map(|op| op_spec_to_proto_op(op) as i32)
            .collect();

        Self {
            groupby,
            fields,
            ops,
            aliases,
        }
    }
}

pub fn op_spec_to_proto_op(op: &AggregateOpSpec) -> AggregateOp {
    match op {
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
    }
}

pub fn op_name(op: AggregateOp) -> String {
    match op {
        AggregateOp::Count => "count",
        AggregateOp::Valid => "valid",
        AggregateOp::Missing => "missing",
        AggregateOp::Distinct => "distinct",
        AggregateOp::Sum => "sum",
        AggregateOp::Product => "product",
        AggregateOp::Mean => "mean",
        AggregateOp::Average => "average",
        AggregateOp::Variance => "variance",
        AggregateOp::Variancep => "variancep",
        AggregateOp::Stdev => "stdev",
        AggregateOp::Stdevp => "stdevp",
        AggregateOp::Stderr => "stderr",
        AggregateOp::Median => "median",
        AggregateOp::Q1 => "q1",
        AggregateOp::Q3 => "q3",
        AggregateOp::Ci0 => "ci0",
        AggregateOp::Ci1 => "ci1",
        AggregateOp::Min => "min",
        AggregateOp::Max => "max",
        AggregateOp::Argmin => "argmin",
        AggregateOp::Argmax => "argmax",
        AggregateOp::Values => "values",
    }
    .to_string()
}

impl TransformDependencies for Aggregate {}
