use crate::proto::gen::transforms::Facet;
use crate::spec::transform::facet::FacetTransformSpec;
use vegafusion_common::error::Result;
use crate::proto::gen::tasks::Variable;
use crate::proto::gen::transforms::{Transform as ProtoTransform, transform::TransformKind};
use crate::task_graph::task::InputVariable;
use crate::transform::TransformDependencies;


impl Facet {
    pub fn try_new(tx: &FacetTransformSpec) -> Result<Facet> {
        Ok(Facet {
            groupby: tx.groupby.clone(),
            transform: tx.transform.iter().map(|tx| {
                let transform_kind = TransformKind::try_from(tx)?;
                Ok(ProtoTransform { transform_kind: Some(transform_kind) })
            }).collect::<Result<Vec<_>>>()?,
        })
    }
}


impl TransformDependencies for Facet {
    fn input_vars(&self) -> Vec<InputVariable> {
        self.transform.iter().flat_map(|tx| tx.input_vars()).collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.transform.iter().flat_map(|tx| tx.output_vars()).collect()
    }
}