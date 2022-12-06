/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
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