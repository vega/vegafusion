use crate::error::Result;
use crate::spec::transform::filter::FilterTransformSpec;
use crate::proto::gen::transforms::Filter;
use crate::expression::parser::parse;

impl Filter {
    pub fn try_new(spec: &FilterTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        Ok(Self { expr: Some(expr) })
    }
}
