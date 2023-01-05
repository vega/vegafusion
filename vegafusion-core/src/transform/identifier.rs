use crate::error::Result;
use crate::proto::gen::transforms::Identifier;
use crate::spec::transform::identifier::IdentifierTransformSpec;
use crate::transform::TransformDependencies;

impl Identifier {
    pub fn try_new(spec: &IdentifierTransformSpec) -> Result<Self> {
        Ok(Self {
            r#as: spec.as_.clone(),
        })
    }
}

impl TransformDependencies for Identifier {}
