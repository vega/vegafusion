use crate::error::Result;
use crate::proto::gen::transforms::Pivot;
use crate::spec::transform::pivot::PivotTransformSpec;
use crate::transform::aggregate::op_spec_to_proto_op;
use crate::transform::TransformDependencies;

impl Pivot {
    pub fn try_new(spec: &PivotTransformSpec) -> Result<Self> {
        let op = spec.op.as_ref().map(|op| op_spec_to_proto_op(op) as i32);
        Ok(Self {
            field: spec.field.clone(),
            value: spec.value.clone(),
            groupby: spec.groupby.clone().unwrap_or_default(),
            limit: spec.limit,
            op,
        })
    }
}

impl TransformDependencies for Pivot {}
