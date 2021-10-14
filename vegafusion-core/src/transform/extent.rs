use crate::proto::gen::transforms::Extent;
use crate::spec::transform::extent::ExtentTransformSpec;

impl Extent {
    pub fn new(spec: &ExtentTransformSpec) -> Self {
        Self {
            field: spec.field.clone(),
            signal: spec.signal.clone(),
        }
    }
}