use crate::error::Result;
use crate::proto::gen::transforms::Fold;
use crate::spec::transform::fold::FoldTransformSpec;
use crate::transform::TransformDependencies;

use crate::task_graph::task::InputVariable;

impl Fold {
    pub fn try_new(spec: &FoldTransformSpec) -> Result<Self> {
        Ok(Self {
            fields: spec.fields.iter().map(|field| field.field()).collect(),
            r#as: spec.as_(),
        })
    }
}

impl TransformDependencies for Fold {
    fn input_vars(&self) -> Vec<InputVariable> {
        Default::default()
    }
}
