use crate::proto::gen::transforms::Extent;
use crate::spec::transform::extent::ExtentTransformSpec;
use crate::transform::TransformDependencies;

impl Extent {
    pub fn new(spec: &ExtentTransformSpec) -> Self {
        Self {
            field: spec.field.clone(),
            signal: spec.signal.clone(),
        }
    }
}

impl TransformDependencies for Extent {
    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}